"""
TPR Ward Cache Module

Caches ward geometries and environmental variables after the initial TPR workflow.
This enables fast combination switching without re-extracting environmental data.
"""

import os
import pickle
import logging
from typing import Dict, Any, Optional

import pandas as pd
import geopandas as gpd

logger = logging.getLogger(__name__)


def get_ward_cache_path(session_id: str) -> str:
    """Get the path to the ward cache file for a session."""
    return f"instance/uploads/{session_id}/ward_cache.pkl"


def save_ward_cache(
    session_id: str,
    state_gdf: gpd.GeoDataFrame,
    env_data: pd.DataFrame,
    state_name: str
) -> bool:
    """
    Cache ward geometries and environmental variables for fast combination switching.

    Args:
        session_id: Session identifier
        state_gdf: GeoDataFrame with ward geometries (filtered to state)
        env_data: DataFrame with environmental variables
        state_name: Name of the state

    Returns:
        True if cache was saved successfully
    """
    cache_path = get_ward_cache_path(session_id)

    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)

        # Prepare cache data
        cache_data = {
            'state_name': state_name,
            'state_gdf': state_gdf,
            'env_data': env_data,
            'ward_count': len(state_gdf),
            'env_columns': list(env_data.columns) if not env_data.empty else []
        }

        # Save to pickle
        with open(cache_path, 'wb') as f:
            pickle.dump(cache_data, f)

        logger.info(f"Ward cache saved: {len(state_gdf)} wards, "
                   f"{len(cache_data['env_columns'])} env vars for {state_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to save ward cache: {e}")
        return False


def load_ward_cache(session_id: str) -> Optional[Dict[str, Any]]:
    """
    Load cached ward geometries and environmental variables.

    Args:
        session_id: Session identifier

    Returns:
        Dict with 'state_gdf', 'env_data', 'state_name', etc. or None if not found
    """
    cache_path = get_ward_cache_path(session_id)

    if not os.path.exists(cache_path):
        logger.warning(f"Ward cache not found at {cache_path}")
        return None

    try:
        with open(cache_path, 'rb') as f:
            cache_data = pickle.load(f)

        logger.info(f"Ward cache loaded: {cache_data.get('ward_count', 0)} wards, "
                   f"{len(cache_data.get('env_columns', []))} env vars")
        return cache_data

    except Exception as e:
        logger.error(f"Failed to load ward cache: {e}")
        return None


def is_ward_cache_available(session_id: str) -> bool:
    """Check if ward cache exists for a session."""
    cache_path = get_ward_cache_path(session_id)
    return os.path.exists(cache_path)
