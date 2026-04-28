"""
Flexible Data Access Layer for ChatMRPT
Handles raw data access, on-demand analysis, and automatic data quality fixes
"""

import os
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple, Union
from pathlib import Path

from ..analysis.imputation import handle_spatial_imputation, handle_mean_imputation
from .data_validation import DataValidator
from ..analysis.pipeline import run_full_analysis_pipeline
from ..analysis.pca_pipeline import run_independent_pca_analysis
from app.services.variable_resolver import variable_resolver

logger = logging.getLogger(__name__)


class FlexibleDataAccess:
    """
    Unified data access that works with raw or analyzed data.
    Automatically handles data quality issues before any analysis.
    """
    
    def __init__(self, session_id: str):
        """
        Initialize flexible data access for a session.
        
        Args:
            session_id: Unique session identifier
        """
        self.session_id = session_id
        self.upload_folder = Path(f"instance/uploads/{session_id}")
        self.session_folder = Path(f"sessions/{session_id}")
        
        # Caches
        self._raw_csv_cache = None
        self._raw_shapefile_cache = None
        self._clean_data_cache = None
        self._composite_cache = None
        self._pca_cache = None
        self._unified_cache = None
        
        # Data quality tracking
        self.data_quality_report = {
            'missing_values_handled': {},
            'ward_mismatches_fixed': [],
            'data_issues_found': [],
            'imputation_methods_used': {}
        }
        
        # Initialize validator
        self.validator = DataValidator()
        
    def get_data_for_request(self, request_type: str, 
                           require_complete_data: bool = True) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Get appropriate data based on request type.
        Automatically handles data quality issues.
        
        Args:
            request_type: Type of request (e.g., 'explore', 'ranking', 'spatial')
            require_complete_data: If True, impute missing values
            
        Returns:
            DataFrame or GeoDataFrame ready for analysis
        """
        logger.info(f"Getting data for request type: {request_type}")
        
        # Determine what level of data is needed
        if request_type in ['explore', 'summary', 'quality_check', 'columns']:
            # Raw data requests - minimal processing
            return self.get_raw_combined_data(clean=False)
            
        elif request_type in ['correlation', 'statistics', 'spatial_basic']:
            # Needs clean data but not full analysis
            return self.get_clean_data(require_complete=require_complete_data)
            
        elif request_type in ['ranking', 'vulnerability', 'composite']:
            # Needs composite analysis
            return self.get_composite_analysis_data()
            
        elif request_type in ['pca', 'dimensions', 'components']:
            # Needs PCA analysis
            return self.get_pca_analysis_data()
            
        elif request_type in ['full', 'comparison', 'comprehensive']:
            # Needs all analyses
            return self.get_unified_analysis_data()
            
        else:
            # Default to clean data
            return self.get_clean_data(require_complete=require_complete_data)
    
    def get_raw_csv_data(self) -> Optional[pd.DataFrame]:
        """Load raw CSV data from upload folder."""
        if self._raw_csv_cache is not None:
            return self._raw_csv_cache
            
        csv_files = list(self.upload_folder.glob("*.csv"))
        if not csv_files:
            logger.warning(f"No CSV files found in {self.upload_folder}")
            return None
            
        # Load the first CSV file found
        csv_path = csv_files[0]
        logger.info(f"Loading raw CSV from: {csv_path}")
        
        try:
            self._raw_csv_cache = pd.read_csv(csv_path)
            logger.info(f"Loaded CSV with {len(self._raw_csv_cache)} rows, {len(self._raw_csv_cache.columns)} columns")
            return self._raw_csv_cache
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            return None
    
    def get_raw_shapefile_data(self) -> Optional[gpd.GeoDataFrame]:
        """Load raw shapefile data from upload folder."""
        if self._raw_shapefile_cache is not None:
            return self._raw_shapefile_cache
            
        # Look for extracted shapefiles
        shp_files = list(self.upload_folder.glob("*.shp"))
        if not shp_files:
            # Look for zip files that might contain shapefiles
            zip_files = list(self.upload_folder.glob("*.zip"))
            if zip_files:
                # Extract first zip file
                import zipfile
                with zipfile.ZipFile(zip_files[0], 'r') as zip_ref:
                    zip_ref.extractall(self.upload_folder)
                shp_files = list(self.upload_folder.glob("*.shp"))
        
        if not shp_files:
            logger.warning(f"No shapefile found in {self.upload_folder}")
            return None
            
        # Load the first shapefile found
        shp_path = shp_files[0]
        logger.info(f"Loading raw shapefile from: {shp_path}")
        
        try:
            self._raw_shapefile_cache = gpd.read_file(shp_path)
            logger.info(f"Loaded shapefile with {len(self._raw_shapefile_cache)} features")
            return self._raw_shapefile_cache
        except Exception as e:
            logger.error(f"Error loading shapefile: {e}")
            return None
    
    def get_raw_combined_data(self, clean: bool = True) -> Optional[gpd.GeoDataFrame]:
        """
        Get combined CSV + shapefile data.
        
        Args:
            clean: If True, handle data quality issues
            
        Returns:
            GeoDataFrame with combined data
        """
        csv_data = self.get_raw_csv_data()
        shp_data = self.get_raw_shapefile_data()
        
        if csv_data is None or shp_data is None:
            logger.error("Cannot combine data - missing CSV or shapefile")
            return None
        
        # Check for required columns
        if 'WardName' not in csv_data.columns or 'WardName' not in shp_data.columns:
            logger.error("WardName column missing in data")
            return None
        
        # Handle ward name mismatches first
        mismatches = self.validator.check_wardname_mismatches(csv_data, shp_data)
        if mismatches:
            logger.warning(f"Found {len(mismatches)} ward name mismatches")
            self.data_quality_report['ward_mismatches_fixed'] = mismatches
            
            # Apply fuzzy matching to fix mismatches
            csv_data = self._fix_ward_name_mismatches(csv_data, shp_data)
        
        # Smart merge strategy: Handle WardName duplicates intelligently
        try:
            # Check for WardName duplicates in either dataset
            csv_dup_names = csv_data[csv_data.duplicated('WardName', keep=False)]['WardName'].unique()
            shp_dup_names = shp_data[shp_data.duplicated('WardName', keep=False)]['WardName'].unique()
            duplicate_names = set(csv_dup_names) | set(shp_dup_names)
            
            if len(duplicate_names) > 0:
                logger.info(f"Found {len(duplicate_names)} ward names with duplicates: {list(duplicate_names)[:3]}...")
                
                # Create display names: WardName (WardCode) for duplicates only
                if 'WardCode' in csv_data.columns and 'WardCode' in shp_data.columns:
                    # For CSV data
                    csv_data = csv_data.copy()
                    csv_data['WardDisplayName'] = csv_data.apply(
                        lambda row: f"{row['WardName']} ({row['WardCode']})" 
                        if row['WardName'] in duplicate_names 
                        else row['WardName'], axis=1
                    )
                    
                    # For Shapefile data  
                    shp_data = shp_data.copy()
                    shp_data['WardDisplayName'] = shp_data.apply(
                        lambda row: f"{row['WardName']} ({row['WardCode']})"
                        if row['WardName'] in duplicate_names
                        else row['WardName'], axis=1
                    )
                    
                    # Merge on WardCode for safety (no duplicates)
                    combined = shp_data.merge(csv_data, on='WardCode', how='inner', suffixes=('_shp', '_csv'))
                    logger.info(f"Smart merge completed: {len(combined)} wards (handled {len(duplicate_names)} duplicate names)")
                    
                    # Use the display name from either side (should be the same)
                    combined['WardDisplayName'] = combined['WardDisplayName_shp']
                    
                else:
                    logger.warning("WardCode not available - cannot handle duplicates properly")
                    # Fallback to regular WardName merge (will create duplicates)
                    combined = shp_data.merge(csv_data, on='WardName', how='inner', suffixes=('_shp', '_csv'))
                    combined['WardDisplayName'] = combined['WardName']
            else:
                # No duplicates - simple WardName merge
                combined = shp_data.merge(csv_data, on='WardName', how='inner', suffixes=('_shp', '_csv'))
                combined['WardDisplayName'] = combined['WardName']
                logger.info(f"Simple WardName merge: {len(combined)} wards (no duplicates)")
            
            # Clean up duplicate columns and ensure WardName is available
            # Keep the best version of WardName for user display
            if 'WardName_shp' in combined.columns:
                combined['WardName'] = combined['WardName_shp']
                combined = combined.drop(['WardName_shp', 'WardName_csv'], axis=1, errors='ignore')
            elif 'WardName' not in combined.columns and 'WardDisplayName' in combined.columns:
                # Extract WardName from WardDisplayName if needed
                combined['WardName'] = combined['WardDisplayName'].str.replace(r'\s*\([^)]*\)', '', regex=True)
            
            # Ensure WardDisplayName is the first column for user convenience
            cols = combined.columns.tolist()
            if 'WardDisplayName' in cols:
                cols.remove('WardDisplayName')
                cols.insert(0, 'WardDisplayName')
                combined = combined[cols]
            
            logger.info(f"Final merged data: {len(combined)} wards")
            
            if clean:
                # Clean data quality issues
                combined = self._handle_data_quality_issues(combined, shp_data)
            
            return combined
            
        except Exception as e:
            logger.error(f"Error combining data: {e}")
            return None
    
    def get_clean_data(self, require_complete: bool = True) -> Optional[gpd.GeoDataFrame]:
        """
        Get cleaned data with quality issues handled.
        
        Args:
            require_complete: If True, ensure no missing values
            
        Returns:
            Clean GeoDataFrame ready for analysis
        """
        if self._clean_data_cache is not None:
            return self._clean_data_cache
        
        # Get raw combined data
        combined = self.get_raw_combined_data(clean=False)
        if combined is None:
            return None
        
        # Handle all data quality issues
        cleaned = self._handle_data_quality_issues(combined, self.get_raw_shapefile_data(), 
                                                   require_complete=require_complete)
        
        self._clean_data_cache = cleaned
        return cleaned
    
    def _handle_data_quality_issues(self, data: gpd.GeoDataFrame, 
                                   shapefile: gpd.GeoDataFrame,
                                   require_complete: bool = True) -> gpd.GeoDataFrame:
        """
        Handle all data quality issues before analysis.
        
        Steps:
        1. Check data types and fix
        2. Handle missing values (spatial imputation)
        3. Remove outliers if needed
        4. Standardize values
        
        Args:
            data: Combined GeoDataFrame
            shapefile: Original shapefile for spatial relationships
            require_complete: If True, impute all missing values
            
        Returns:
            Clean GeoDataFrame
        """
        logger.info("Handling data quality issues...")
        cleaned = data.copy()
        
        # Step 1: Fix data types
        cleaned = self._fix_data_types(cleaned)
        
        # Step 2: Handle missing values if required
        if require_complete:
            # Identify numeric columns with missing values
            numeric_cols = cleaned.select_dtypes(include=[np.number]).columns
            missing_cols = [col for col in numeric_cols if cleaned[col].isnull().any()]
            
            if missing_cols:
                logger.info(f"Found missing values in {len(missing_cols)} columns")
                
                # Use spatial imputation for numeric columns
                for col in missing_cols:
                    missing_count = cleaned[col].isnull().sum()
                    if missing_count > 0:
                        logger.info(f"Imputing {missing_count} missing values in '{col}' using spatial neighbors")
                        
                        # Try spatial imputation first
                        cleaned = handle_spatial_imputation(cleaned, col, shapefile)
                        
                        # Check if any missing values remain
                        remaining_missing = cleaned[col].isnull().sum()
                        if remaining_missing > 0:
                            # Fall back to mean imputation
                            logger.warning(f"Spatial imputation incomplete for '{col}', using mean for {remaining_missing} values")
                            cleaned = handle_mean_imputation(cleaned, col)
                        
                        # Record imputation
                        self.data_quality_report['missing_values_handled'][col] = {
                            'original_missing': missing_count,
                            'method': 'spatial_then_mean' if remaining_missing > 0 else 'spatial',
                            'values_imputed': missing_count - cleaned[col].isnull().sum()
                        }
        
        # Step 3: Check for and handle outliers (optional)
        cleaned = self._handle_outliers(cleaned)
        
        # Step 4: Validate final data
        validation_issues = self._validate_cleaned_data(cleaned)
        if validation_issues:
            self.data_quality_report['data_issues_found'] = validation_issues
            logger.warning(f"Data quality issues remain: {validation_issues}")
        
        logger.info("Data quality handling complete")
        return cleaned
    
    def _fix_data_types(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Fix common data type issues."""
        fixed = data.copy()
        
        # Common fixes
        for col in fixed.columns:
            if col in ['StateCode', 'WardCode', 'LGACode']:
                # These should be strings, not numbers
                fixed[col] = fixed[col].astype(str)
            elif col == 'Urban':
                # Convert Yes/No to boolean
                if fixed[col].dtype == 'object':
                    fixed[col] = fixed[col].map({'Yes': 1, 'No': 0})
        
        return fixed
    
    def _fix_ward_name_mismatches(self, csv_data: pd.DataFrame, 
                                  shp_data: gpd.GeoDataFrame) -> pd.DataFrame:
        """
        Fix ward name mismatches using fuzzy matching.
        
        Args:
            csv_data: CSV data with potential mismatches
            shp_data: Shapefile with standard ward names
            
        Returns:
            CSV data with corrected ward names
        """
        from difflib import get_close_matches
        
        csv_wards = set(csv_data['WardName'].unique())
        shp_wards = set(shp_data['WardName'].unique())
        
        # Find mismatches
        csv_only = csv_wards - shp_wards
        
        if not csv_only:
            return csv_data
        
        # Create mapping for fixes
        ward_mapping = {}
        
        for ward in csv_only:
            # Find closest match in shapefile
            matches = get_close_matches(ward, shp_wards, n=1, cutoff=0.8)
            if matches:
                ward_mapping[ward] = matches[0]
                logger.info(f"Mapping '{ward}' -> '{matches[0]}'")
        
        # Apply mapping
        if ward_mapping:
            csv_data['WardName'] = csv_data['WardName'].replace(ward_mapping)
            logger.info(f"Fixed {len(ward_mapping)} ward name mismatches")
        
        return csv_data
    
    def _handle_outliers(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Handle outliers in numeric columns (optional)."""
        # For now, just log potential outliers
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if col != 'geometry':
                q1 = data[col].quantile(0.25)
                q3 = data[col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                outliers = ((data[col] < lower_bound) | (data[col] > upper_bound)).sum()
                if outliers > 0:
                    logger.info(f"Column '{col}' has {outliers} potential outliers")
        
        return data
    
    def _validate_cleaned_data(self, data: gpd.GeoDataFrame) -> List[str]:
        """Validate cleaned data and return any remaining issues."""
        issues = []
        
        # Check for remaining missing values
        missing_summary = data.isnull().sum()
        if missing_summary.any():
            issues.append(f"Missing values remain: {missing_summary[missing_summary > 0].to_dict()}")
        
        # Check for required columns
        required_cols = ['WardName', 'geometry']
        missing_required = [col for col in required_cols if col not in data.columns]
        if missing_required:
            issues.append(f"Missing required columns: {missing_required}")
        
        # Check geometry validity
        invalid_geom = (~data.geometry.is_valid).sum()
        if invalid_geom > 0:
            issues.append(f"{invalid_geom} invalid geometries found")
        
        return issues
    
    def get_composite_analysis_data(self) -> Optional[gpd.GeoDataFrame]:
        """Get data with composite analysis results."""
        if self._composite_cache is not None:
            return self._composite_cache
        
        # Get clean data first
        clean_data = self.get_clean_data(require_complete=True)
        if clean_data is None:
            return None
        
        logger.info("Running composite analysis on-demand...")
        
        try:
            # Create a temporary DataHandler for analysis
            from .data_handler import DataHandler
            temp_handler = DataHandler(str(self.session_folder))
            temp_handler._csv_data = clean_data.drop('geometry', axis=1)
            temp_handler._shapefile_data = clean_data[['WardName', 'geometry']]
            
            # Run analysis
            results = run_full_analysis_pipeline(data_handler=temp_handler)
            
            if results['status'] == 'success':
                # Merge results back with geographic data
                composite_scores = results['unified_dataset'][['WardName', 'Composite_Score', 'Rank']]
                self._composite_cache = clean_data.merge(composite_scores, on='WardName', how='left')
                
                logger.info("Composite analysis complete")
                return self._composite_cache
            else:
                logger.error(f"Composite analysis failed: {results.get('message')}")
                return None
                
        except Exception as e:
            logger.error(f"Error running composite analysis: {e}")
            return None
    
    def get_pca_analysis_data(self) -> Optional[gpd.GeoDataFrame]:
        """Get data with PCA analysis results."""
        if self._pca_cache is not None:
            return self._pca_cache
        
        # Get clean data first
        clean_data = self.get_clean_data(require_complete=True)
        if clean_data is None:
            return None
        
        logger.info("Running PCA analysis on-demand...")
        
        try:
            # Prepare numeric columns for PCA
            numeric_cols = clean_data.select_dtypes(include=[np.number]).columns
            exclude_cols = ['Rank', 'Composite_Score'] if 'Composite_Score' in clean_data.columns else []
            pca_cols = [col for col in numeric_cols if col not in exclude_cols]
            
            # Run PCA
            pca_results = run_independent_pca_analysis(
                data=clean_data,
                feature_columns=pca_cols,
                n_components=None  # Let it determine optimal components
            )
            
            if pca_results['status'] == 'success':
                # Merge PCA scores with data
                pca_scores = pd.DataFrame(pca_results['pca_scores'])
                pca_scores['WardName'] = clean_data['WardName']
                
                self._pca_cache = clean_data.merge(pca_scores, on='WardName', how='left')
                logger.info("PCA analysis complete")
                return self._pca_cache
            else:
                logger.error(f"PCA analysis failed: {pca_results.get('message')}")
                return None
                
        except Exception as e:
            logger.error(f"Error running PCA analysis: {e}")
            return None
    
    def get_unified_analysis_data(self) -> Optional[gpd.GeoDataFrame]:
        """Get data with all analyses (composite + PCA)."""
        if self._unified_cache is not None:
            return self._unified_cache
        
        # Run both analyses
        composite_data = self.get_composite_analysis_data()
        if composite_data is None:
            return None
        
        pca_data = self.get_pca_analysis_data()
        if pca_data is None:
            return composite_data  # Return at least composite
        
        # Merge both results
        try:
            pca_cols = [col for col in pca_data.columns if col.startswith('PC')]
            unified = composite_data.copy()
            
            for col in pca_cols:
                unified[col] = pca_data[col]
            
            self._unified_cache = unified
            return unified
            
        except Exception as e:
            logger.error(f"Error creating unified dataset: {e}")
            return composite_data
    
    def get_data_quality_report(self) -> Dict[str, Any]:
        """Get comprehensive data quality report."""
        report = self.data_quality_report.copy()
        
        # Add current data status
        csv_data = self.get_raw_csv_data()
        shp_data = self.get_raw_shapefile_data()
        
        report['data_status'] = {
            'csv_loaded': csv_data is not None,
            'csv_rows': len(csv_data) if csv_data is not None else 0,
            'csv_columns': len(csv_data.columns) if csv_data is not None else 0,
            'shapefile_loaded': shp_data is not None,
            'shapefile_features': len(shp_data) if shp_data is not None else 0
        }
        
        # Add cache status
        report['cache_status'] = {
            'raw_data_cached': self._raw_csv_cache is not None,
            'clean_data_cached': self._clean_data_cache is not None,
            'composite_cached': self._composite_cache is not None,
            'pca_cached': self._pca_cache is not None
        }
        
        return report
    
    def invalidate_cache(self, cache_type: str = 'all'):
        """
        Invalidate cached data.
        
        Args:
            cache_type: 'all', 'analysis', or specific cache name
        """
        if cache_type == 'all':
            self._raw_csv_cache = None
            self._raw_shapefile_cache = None
            self._clean_data_cache = None
            self._composite_cache = None
            self._pca_cache = None
            self._unified_cache = None
        elif cache_type == 'analysis':
            self._composite_cache = None
            self._pca_cache = None
            self._unified_cache = None
        else:
            setattr(self, f"_{cache_type}_cache", None)
        
        logger.info(f"Cache invalidated: {cache_type}")