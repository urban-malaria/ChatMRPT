"""
Data Path Configuration for ChatMRPT

This module centralizes all data path configurations to ensure
the application works both locally and on AWS.
"""

import os
from pathlib import Path

# Get the project root directory dynamically
_current_file = Path(__file__).resolve()
PROJECT_ROOT = _current_file.parent.parent.parent  # Go up from app/config/

# For AWS deployment, these paths might be overridden by environment variables
# or mounted from S3/EFS

# Raster data directory
RASTER_DIR = os.environ.get('CHATMRPT_RASTER_DIR', 
                            os.path.join(PROJECT_ROOT, 'rasters'))

# Nigeria master shapefile
NIGERIA_SHAPEFILE = os.environ.get('CHATMRPT_NIGERIA_SHAPEFILE',
                                   os.path.join(PROJECT_ROOT, 'www', 'complete_names_wards', 'wards.shp'))

# TPR data directory
TPR_DATA_DIR = os.environ.get('CHATMRPT_TPR_DATA_DIR',
                              os.path.join(PROJECT_ROOT, 'www', 'tpr_data_by_state'))

# Settlement data directory
SETTLEMENT_DATA_DIR = os.environ.get('CHATMRPT_SETTLEMENT_DIR',
                                     os.path.join(PROJECT_ROOT, 'kano_settlement_data'))

# Instance directory for runtime data
INSTANCE_DIR = os.environ.get('CHATMRPT_INSTANCE_DIR',
                              os.path.join(PROJECT_ROOT, 'instance'))

# Upload directory
UPLOAD_DIR = os.environ.get('CHATMRPT_UPLOAD_DIR',
                            os.path.join(INSTANCE_DIR, 'uploads'))

# Population rasters for burden calculation
GEOSPATIAL_DIR = os.path.join(PROJECT_ROOT, 'data', 'geospatial')
POP_TOTAL_RASTER = os.path.join(GEOSPATIAL_DIR, 'nigeria.tif')
POP_U5_RASTER = os.path.join(GEOSPATIAL_DIR, 'NGA_population_v2_0_agesex_under5.tif')
POP_F15_49_RASTER = os.path.join(GEOSPATIAL_DIR, 'NGA_population_v2_0_agesex', 'NGA_population_v2_0_agesex_f15_49.tif')


def get_raster_path(raster_name: str) -> str:
    """
    Get the full path to a raster file.
    
    Args:
        raster_name: Name or relative path of the raster
        
    Returns:
        Full path to the raster file
    """
    return os.path.join(RASTER_DIR, raster_name)


def get_tpr_file(state_code: str) -> str:
    """
    Get the path to a state's TPR data file.
    
    Args:
        state_code: State code (e.g., 'ad' for Adamawa)
        
    Returns:
        Path to the TPR file
    """
    import glob
    pattern = os.path.join(TPR_DATA_DIR, f"{state_code}_*.xlsx")
    files = glob.glob(pattern)
    return files[0] if files else None


def ensure_directories_exist():
    """
    Ensure all required directories exist.
    Called during application startup.
    """
    directories = [
        INSTANCE_DIR,
        UPLOAD_DIR,
        os.path.join(INSTANCE_DIR, 'variable_cache'),
        os.path.join(INSTANCE_DIR, 'reports'),
        os.path.join(INSTANCE_DIR, 'visualizations')
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)


# For AWS deployment considerations:
# 1. Raster files could be stored in S3 and accessed via boto3
# 2. Shapefiles could be in EFS (Elastic File System) 
# 3. Instance data should be in EFS for persistence across instances
# 4. Environment variables can be set in the EC2 user data or ECS task definition

# Example AWS setup:
# export CHATMRPT_RASTER_DIR=/mnt/efs/chatmrpt/rasters
# export CHATMRPT_NIGERIA_SHAPEFILE=/mnt/efs/chatmrpt/shapefiles/nigeria/wards.shp
# export CHATMRPT_INSTANCE_DIR=/mnt/efs/chatmrpt/instance