"""
Configuration management for ChatMRPT application.

This module provides environment-based configuration classes that can be
easily switched between development, testing, and production environments.
"""

import os
from .base import BaseConfig
from .development import DevelopmentConfig
from .production import ProductionConfig
from .testing import TestingConfig

# Configuration mapping
config_map = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}

def get_config(config_name=None):
    """
    Get configuration class based on environment name.
    
    Args:
        config_name (str): Name of the configuration environment
        
    Returns:
        Config class for the specified environment
    """
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    return config_map.get(config_name, config_map['default'])

__all__ = [
    'BaseConfig',
    'DevelopmentConfig', 
    'ProductionConfig',
    'TestingConfig',
    'get_config'
] 