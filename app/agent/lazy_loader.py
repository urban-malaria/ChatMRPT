"""
Lazy Data Loader for Data Analysis V3
Provides on-demand data loading to avoid memory overhead
"""

import os
import logging
import pandas as pd
from .encoding_handler import EncodingHandler
from typing import Dict, Any

logger = logging.getLogger(__name__)


class LazyDataLoader:
    """
    Lazy loader that only loads data when accessed.
    Provides a proxy object that loads data on first access.
    """
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.data_dir = f"instance/uploads/{session_id}"
        self._loaded_data = {}  # Cache for loaded dataframes
        self._file_paths = {}   # Map of variable names to file paths
        
        # Scan for available files
        self._scan_files()
    
    def _scan_files(self):
        """Scan the session directory for data files."""
        if not os.path.exists(self.data_dir):
            return
        
        for file in os.listdir(self.data_dir):
            if file.endswith('.csv'):
                var_name = file.replace('.csv', '').replace(' ', '_').replace('-', '_')
                self._file_paths[var_name] = os.path.join(self.data_dir, file)
                # Also make available as 'df' for convenience
                if 'df' not in self._file_paths:
                    self._file_paths['df'] = os.path.join(self.data_dir, file)
            
            elif file.endswith(('.xlsx', '.xls')):
                var_name = file.split('.')[0].replace(' ', '_').replace('-', '_')
                self._file_paths[var_name] = os.path.join(self.data_dir, file)
                # Also make available as 'df' for convenience
                if 'df' not in self._file_paths:
                    self._file_paths['df'] = os.path.join(self.data_dir, file)
    
    def get_variable(self, name: str):
        """
        Get a variable by name, loading it if necessary.
        
        Args:
            name: Variable name
            
        Returns:
            DataFrame or None if not found
        """
        # Check if already loaded
        if name in self._loaded_data:
            return self._loaded_data[name]
        
        # Check if file path exists
        if name not in self._file_paths:
            return None
        
        # Load the data
        filepath = self._file_paths[name]
        try:
            if filepath.endswith('.csv'):
                df = EncodingHandler.read_csv_with_encoding(filepath)
            else:
                df = EncodingHandler.read_excel_with_encoding(filepath)
            
            # Cache it
            self._loaded_data[name] = df
            logger.info(f"Lazy loaded {name} from {filepath} (shape: {df.shape})")
            
            return df
            
        except Exception as e:
            logger.error(f"Error lazy loading {name}: {e}")
            return None
    
    def get_available_variables(self):
        """Get list of available variable names."""
        return list(self._file_paths.keys())
    
    def preload_all(self):
        """
        Preload all data files (for compatibility with existing code).
        Use sparingly - defeats the purpose of lazy loading!
        """
        result = {}
        for var_name in self._file_paths:
            df = self.get_variable(var_name)
            if df is not None:
                result[var_name] = df
        return result
    
    def get_lazy_context(self):
        """
        Get a context dictionary with lazy-loaded variables.
        Returns a special dict that loads data on access.
        """
        return LazyDataContext(self)


class LazyDataContext(dict):
    """
    A dictionary that lazy-loads data when accessed.
    """
    
    def __init__(self, loader: LazyDataLoader):
        super().__init__()
        self.loader = loader
        self._available = loader.get_available_variables()
    
    def __getitem__(self, key):
        # Check if it's a known data variable
        if key in self._available and key not in self:
            # Lazy load the data
            data = self.loader.get_variable(key)
            if data is not None:
                self[key] = data
                return data
            else:
                raise KeyError(f"Failed to load variable: {key}")
        
        # Otherwise, use normal dict behavior
        return super().__getitem__(key)
    
    def __contains__(self, key):
        # Report that we have the key if it's available to load
        return key in self._available or super().__contains__(key)
    
    def keys(self):
        # Return all available keys (loaded and not loaded)
        return set(list(super().keys()) + self._available)
    
    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default