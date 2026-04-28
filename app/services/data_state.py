"""
Unified Data State Manager for ChatMRPT

Following py-sidebot's architecture, this provides a single source of truth
for session data, eliminating the fragmented data loading issues.

Key principles:
1. Single data source for all tools
2. Lazy loading with caching
3. Automatic detection of analysis completion
4. Simple, clear API
"""

import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd
import geopandas as gpd

logger = logging.getLogger(__name__)


class UnifiedDataState:
    """
    Single source of truth for session data.
    
    This class manages all data access for a session, providing:
    - Lazy loading from disk
    - Automatic unified dataset detection
    - Consistent state across all tools
    - Clear analysis stage tracking
    """
    
    def __init__(self, session_id: str, base_upload_folder: str = "instance/uploads"):
        self.session_id = session_id
        self.base_upload_folder = base_upload_folder
        self.session_folder = Path(base_upload_folder) / session_id
        
        # Cached data
        self._raw_data: Optional[pd.DataFrame] = None
        self._shapefile_data: Optional[gpd.GeoDataFrame] = None
        self._unified_data: Optional[gpd.GeoDataFrame] = None
        
        # State tracking
        self._analysis_complete = False
        self._data_loaded = False
        
        # Initialize by checking what's available
        self._check_available_data()
    
    def _check_available_data(self):
        """Check what data is available in the session folder."""
        if not self.session_folder.exists():
            logger.debug(f"Session folder does not exist: {self.session_folder}")
            return
        
        # Check for raw data
        raw_csv = self.session_folder / "raw_data.csv"
        if raw_csv.exists():
            self._data_loaded = True
            logger.info(f"Raw data available for session {self.session_id}")
        
        # Check for analysis completion marker (most reliable)
        marker_file = self.session_folder / ".analysis_complete"
        if marker_file.exists():
            self._analysis_complete = True
            logger.info(f"Analysis complete marker found for session {self.session_id}")
        
        # Check for unified dataset (indicates analysis complete)
        unified_files = [
            self.session_folder / "unified_dataset.geoparquet",
            self.session_folder / "unified_dataset.csv",
            self.session_folder / "unified_dataset.pkl"
        ]
        
        if any(f.exists() for f in unified_files):
            self._analysis_complete = True
            logger.info(f"Analysis complete for session {self.session_id} - unified dataset available")
        
        # Also check for analysis result files
        analysis_result_files = [
            self.session_folder / "analysis_composite_scores.csv",
            self.session_folder / "analysis_vulnerability_rankings.csv",
            self.session_folder / "composite_scores.csv",
            self.session_folder / "analysis_results_composite.csv",
            self.session_folder / "analysis_results_pca.csv"
        ]
        
        if any(f.exists() for f in analysis_result_files):
            self._analysis_complete = True
            logger.info(f"Analysis complete for session {self.session_id} - analysis result files found")
    
    @property
    def current_data(self) -> Optional[pd.DataFrame]:
        """
        Get the current dataset - unified if available, otherwise raw.
        
        This is the main API that all tools should use.
        """
        # If analysis is complete, return unified data
        if self._analysis_complete:
            return self.unified_data
        
        # Otherwise return raw data
        return self.raw_data
    
    @property
    def raw_data(self) -> Optional[pd.DataFrame]:
        """Get raw uploaded data with lazy loading."""
        if self._raw_data is None and self._data_loaded:
            self._load_raw_data()
        return self._raw_data
    
    @property
    def unified_data(self) -> Optional[gpd.GeoDataFrame]:
        """Get unified dataset with lazy loading."""
        if self._unified_data is None and self._analysis_complete:
            self._load_unified_data()
        return self._unified_data
    
    @property
    def analysis_complete(self) -> bool:
        """Check if analysis has been completed."""
        return self._analysis_complete
    
    @property
    def data_loaded(self) -> bool:
        """Check if any data has been loaded."""
        return self._data_loaded
    
    def _load_raw_data(self):
        """Load raw data from disk."""
        try:
            # Try processed data first
            processed_csv = self.session_folder / "processed_data.csv"
            if processed_csv.exists():
                try:
                    self._raw_data = pd.read_csv(processed_csv)
                    logger.info(f"Loaded processed data: {self._raw_data.shape}")
                    return
                except UnicodeDecodeError:
                    # Might be Excel file with .csv extension
                    self._raw_data = pd.read_excel(processed_csv)
                    logger.info(f"Loaded processed data as Excel: {self._raw_data.shape}")
                    return
            
            # Fall back to raw data
            raw_csv = self.session_folder / "raw_data.csv"
            if raw_csv.exists():
                try:
                    self._raw_data = pd.read_csv(raw_csv)
                    logger.info(f"Loaded raw data: {self._raw_data.shape}")
                    return
                except UnicodeDecodeError:
                    # Might be Excel file with .csv extension (TPR files)
                    self._raw_data = pd.read_excel(raw_csv)
                    logger.info(f"Loaded raw data as Excel: {self._raw_data.shape}")
                    return
                
        except Exception as e:
            logger.error(f"Error loading raw data: {e}")
            self._raw_data = None
    
    def _load_unified_data(self):
        """Load unified dataset from disk."""
        try:
            # Try geoparquet first (preferred)
            geoparquet_path = self.session_folder / "unified_dataset.geoparquet"
            if geoparquet_path.exists():
                self._unified_data = gpd.read_parquet(geoparquet_path)
                logger.info(f"Loaded unified dataset from geoparquet: {self._unified_data.shape}")
                return
            
            # Try CSV backup
            csv_path = self.session_folder / "unified_dataset.csv"
            if csv_path.exists():
                # Load CSV
                df = pd.read_csv(csv_path)
                
                # Try to load shapefile to get geometry
                shapefile_dir = self.session_folder / "shapefile"
                if shapefile_dir.exists():
                    shp_files = list(shapefile_dir.glob("*.shp"))
                    if shp_files:
                        try:
                            gdf = gpd.read_file(shp_files[0])
                            # Merge geometry with CSV data
                            # This is simplified - in reality would need proper join logic
                            self._unified_data = gpd.GeoDataFrame(df)
                            logger.info(f"Loaded unified dataset from CSV: {self._unified_data.shape}")
                            return
                        except:
                            pass
                
                # No geometry available, use as regular DataFrame
                self._unified_data = gpd.GeoDataFrame(df)
                logger.info(f"Loaded unified dataset from CSV (no geometry): {self._unified_data.shape}")
                return
                
        except Exception as e:
            logger.error(f"Error loading unified data: {e}")
            self._unified_data = None
    
    def reload_data(self):
        """Force reload of all data from disk."""
        self._raw_data = None
        self._unified_data = None
        self._shapefile_data = None
        self._check_available_data()
        
        logger.info(f"Reloaded data state for session {self.session_id}")
    
    def on_analysis_complete(self):
        """
        Called when analysis completes to update state.
        
        This triggers immediate loading of the unified dataset
        and updates all state flags.
        """
        logger.info(f"Analysis complete for session {self.session_id}")
        
        # Update state
        self._analysis_complete = True
        
        # Force reload to get unified dataset
        self._unified_data = None
        self._load_unified_data()
        
        if self._unified_data is not None:
            logger.info(f"Successfully loaded unified dataset after analysis: {self._unified_data.shape}")
        else:
            logger.error("Failed to load unified dataset after analysis completion")
    
    def get_data_info(self) -> Dict[str, Any]:
        """Get comprehensive information about available data."""
        info = {
            'session_id': self.session_id,
            'data_loaded': self._data_loaded,
            'analysis_complete': self._analysis_complete,
            'stage': self.get_stage()
        }
        
        if self.current_data is not None:
            df = self.current_data
            info.update({
                'shape': df.shape,
                'columns': df.columns.tolist(),
                'dtypes': df.dtypes.to_dict()
            })
            
            # Add analysis-specific info if available
            if self._analysis_complete:
                analysis_cols = ['composite_score', 'composite_rank', 'pca_score', 'pca_rank']
                info['analysis_columns'] = [col for col in analysis_cols if col in df.columns]
        
        return info
    
    def get_stage(self) -> str:
        """Get current data stage."""
        if not self._data_loaded:
            return 'no_data'
        elif self._analysis_complete:
            return 'post_analysis'
        else:
            return 'pre_analysis'


class UnifiedDataStateManager:
    """
    Manager for UnifiedDataState instances across sessions.
    
    This provides a singleton-like interface for getting data states,
    ensuring we don't create multiple instances for the same session.
    """
    
    def __init__(self, base_upload_folder: str = "instance/uploads"):
        self.base_upload_folder = base_upload_folder
        self._states: Dict[str, UnifiedDataState] = {}
    
    def get_state(self, session_id: str) -> UnifiedDataState:
        """Get or create data state for session."""
        if session_id not in self._states:
            self._states[session_id] = UnifiedDataState(
                session_id, 
                self.base_upload_folder
            )
        return self._states[session_id]
    
    def clear_state(self, session_id: str):
        """Clear state for a session."""
        if session_id in self._states:
            del self._states[session_id]
    
    def on_analysis_complete(self, session_id: str):
        """Notify that analysis is complete for a session."""
        state = self.get_state(session_id)
        state.on_analysis_complete()


# Global instance
_data_state_manager = None

def get_data_state_manager() -> UnifiedDataStateManager:
    """Get the global data state manager instance."""
    global _data_state_manager
    if _data_state_manager is None:
        _data_state_manager = UnifiedDataStateManager()
    return _data_state_manager

def get_data_state(session_id: str) -> UnifiedDataState:
    """Convenience function to get data state for a session."""
    manager = get_data_state_manager()
    return manager.get_state(session_id)