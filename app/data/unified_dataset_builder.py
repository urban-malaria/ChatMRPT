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
            
            if data_handler.csv_data is not None:
                sources['csv_data'] = data_handler.csv_data
                print(f"📊 CSV loaded: {data_handler.csv_data.shape[0]} rows, {data_handler.csv_data.shape[1]} columns")
            
            # Load shapefile data using the shapefile_loader
            if hasattr(data_handler, 'shapefile_data') and data_handler.shapefile_data is not None:
                sources['shapefile_data'] = data_handler.shapefile_data
                print(f"🗺️ Shapefile loaded: {data_handler.shapefile_data.shape[0]} features")
            elif hasattr(data_handler, 'shapefile_loader'):
                # Try to load via shapefile_loader
                shp_data = getattr(data_handler.shapefile_loader, 'data', None)
                if shp_data is not None:
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
                    sources['composite_results'] = pd.read_csv(filepath)
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
                    sources['model_formulas'] = pd.read_csv(filepath)
                    print(f"📋 Model formulas loaded: {len(sources['model_formulas'])} model definitions from {pattern}")
                    break
            
            # Dynamically load comprehensive PCA results
            self._load_pca_results_dynamically(sources)
            
            # Validate minimum requirements
            if sources['csv_data'] is None:
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
            'analysis_pca_rankings.csv',
            'pca_vulnerability_rankings.csv',
            'pca_analysis_results.csv',
            'pca_rankings.csv',
            'pca_results.csv'
        ]
        
        for pattern in pca_ranking_patterns:
            filepath = os.path.join(self.session_folder, pattern)
            if os.path.exists(filepath):
                sources['pca_results'] = pd.read_csv(filepath)
                print(f"🔍 PCA rankings loaded: {pattern}")
                break
        
        # Search for PCA components with flexible naming
        pca_component_patterns = [
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
                sources['pca_loadings'] = pd.read_csv(filepath)
                print(f"📊 PCA loadings loaded: {pattern}")
                break
        
        # If no PCA results found, try to generate them dynamically
        if not any([sources.get('pca_results'), sources.get('pca_components')]):
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
                    
                    if pca_result and pca_result.get('status') == 'success':
                        self._extract_pca_results_dynamically(pca_result, sources)
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
                    
                    if sources['pca_results'] is not None:
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
                    
                    if sources['pca_components'] is not None:
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
                    
                    if sources['pca_loadings'] is not None:
                        print(f"✅ Extracted PCA loadings from '{key}'")
                        break
                        
        except Exception as e:
            print(f"⚠️ Error extracting PCA results: {e}")
            logger.warning(f"PCA extraction error: {e}")
    
    def _create_base_dataset(self, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
        """Create base unified geodataframe preserving original column names"""
        
        csv_df = data_sources['csv_data'].copy()
        
        # Only minimal cleaning - preserve original names
        csv_df.columns = csv_df.columns.str.strip()  # Remove leading/trailing spaces only
        print(f"🔧 Preserved original column names: {list(csv_df.columns[:5])}...")
        
        # Merge with shapefile if available
        if data_sources['shapefile_data'] is not None:
            shp_gdf = data_sources['shapefile_data'].copy()
            
            # Find matching key columns
            csv_key = self._detect_ward_key_column(csv_df)
            shp_key = self._detect_ward_key_column(shp_gdf)
            
            if csv_key and shp_key:
                # SMART MERGE: Handle duplicate ward name mismatches
                unified_gdf = self._smart_merge_with_duplicates(shp_gdf, csv_df, shp_key, csv_key)
                print(f"🔗 Smart merged CSV and shapefile: {unified_gdf.shape[0]} wards matched")
            else:
                print("⚠️ Could not match CSV and shapefile - using CSV only")
                unified_gdf = gpd.GeoDataFrame(csv_df)
        else:
            # CSV only - convert to GeoDataFrame
            unified_gdf = gpd.GeoDataFrame(csv_df)
            print("📊 Using CSV data only (no shapefile)")
        
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
        """Detect the ward identifier column"""
        
        possible_keys = []
        for col in df.columns:
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in ['ward', 'name']):
                if df[col].dtype == 'object' and df[col].nunique() > df.shape[0] * 0.8:
                    possible_keys.append(col)
        
        # Return the best match
        if possible_keys:
            # Prefer exact matches
            for col in possible_keys:
                if 'ward' in col.lower() and 'name' in col.lower():
                    return col
            return possible_keys[0]
        
        return None
    
    def _integrate_analysis_results(self, gdf: gpd.GeoDataFrame, data_sources: Dict[str, Any]) -> gpd.GeoDataFrame:
        """Integrate composite and PCA analysis results preserving original names"""
        
        ward_key = self._detect_ward_key_column(gdf)
        if not ward_key:
            print("⚠️ No ward key found - skipping analysis integration")
            return gdf
        
        # Integrate composite results
        if data_sources['composite_results'] is not None:
            comp_df = data_sources['composite_results']
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
                
                # Merge with standardized column names
                gdf = gdf.merge(
                    comp_df_mapped[merge_cols],
                    left_on=ward_key,
                    right_on=comp_key,
                    how='left',
                    suffixes=('', '_comp')
                )
                
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
        if data_sources['pca_results'] is not None:
            pca_df = data_sources['pca_results']
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
                
                # Merge with standardized column names
                gdf = gdf.merge(
                    pca_df_mapped[merge_cols],
                    left_on=ward_key,
                    right_on=pca_key,
                    how='left',
                    suffixes=('', '_pca_dup')
                )
                
                print(f"🔍 Integrated PCA analysis: {len(analysis_cols)} columns -> {', '.join(analysis_cols) if analysis_cols else 'none'}")
        
        return gdf
    
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


def load_unified_dataset(session_id: str) -> Optional[gpd.GeoDataFrame]:
    """Load unified dataset from session folder"""
    
    session_folder = f"instance/uploads/{session_id}"
    
    # Try GeoParquet first
    geoparquet_path = os.path.join(session_folder, 'unified_dataset.geoparquet')
    if os.path.exists(geoparquet_path):
        try:
            return gpd.read_parquet(geoparquet_path)
        except Exception:
            pass
    
    # Try pickle fallback
    pickle_path = os.path.join(session_folder, 'unified_dataset.pkl')
    if os.path.exists(pickle_path):
        return pd.read_pickle(pickle_path)
    
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