"""
Dependency Validation System for ChatMRPT

Provides robust validation of required dependencies for different features,
with graceful fallbacks and user-friendly error messages.
"""

import logging
import importlib
import os
import sys
from typing import Dict, Any, List, Optional, Tuple
from functools import wraps

logger = logging.getLogger(__name__)

class DependencyValidator:
    """
    Validates and manages dependencies for different ChatMRPT features.
    """
    
    def __init__(self):
        self.dependency_cache = {}
        self.validation_results = {}
    
    def validate_geospatial_dependencies(self) -> Dict[str, Any]:
        """Validate geospatial analysis dependencies."""
        if 'geospatial' in self.validation_results:
            return self.validation_results['geospatial']
        
        required_packages = {
            'geopandas': {
                'name': 'GeoPandas',
                'purpose': 'Geospatial data processing',
                'critical': True
            },
            'shapely': {
                'name': 'Shapely',
                'purpose': 'Geometric operations',
                'critical': True
            },
            'fiona': {
                'name': 'Fiona',
                'purpose': 'Vector data I/O',
                'critical': True
            },
            'contextily': {
                'name': 'Contextily',
                'purpose': 'Basemap integration',
                'critical': False
            }
        }
        
        result = self._validate_package_group(required_packages, 'geospatial analysis')
        self.validation_results['geospatial'] = result
        return result
    
    def validate_visualization_dependencies(self) -> Dict[str, Any]:
        """Validate visualization dependencies."""
        if 'visualization' in self.validation_results:
            return self.validation_results['visualization']
        
        required_packages = {
            'plotly': {
                'name': 'Plotly',
                'purpose': 'Interactive visualizations',
                'critical': True
            },
            'matplotlib': {
                'name': 'Matplotlib',
                'purpose': 'Static plots',
                'critical': False
            }
        }
        
        result = self._validate_package_group(required_packages, 'visualization')
        self.validation_results['visualization'] = result
        return result
    
    def validate_settlement_dependencies(self) -> Dict[str, Any]:
        """Validate settlement analysis dependencies."""
        if 'settlement' in self.validation_results:
            return self.validation_results['settlement']
        
        # Settlement analysis requires both geospatial and visualization
        geo_result = self.validate_geospatial_dependencies()
        viz_result = self.validate_visualization_dependencies()
        
        if geo_result['status'] == 'error' or viz_result['status'] == 'error':
            result = {
                'status': 'error',
                'message': 'Settlement analysis requires both geospatial and visualization dependencies',
                'missing_packages': (geo_result.get('missing_packages', []) + 
                                   viz_result.get('missing_packages', [])),
                'user_message': 'Settlement analysis is not available. Required geospatial or visualization libraries are missing.',
                'suggestions': self._get_installation_suggestions(['geopandas', 'plotly'])
            }
        else:
            result = {
                'status': 'success',
                'message': 'All settlement analysis dependencies are available',
                'available_packages': (geo_result.get('available_packages', []) + 
                                     viz_result.get('available_packages', []))
            }
        
        self.validation_results['settlement'] = result
        return result
    
    def validate_ai_dependencies(self) -> Dict[str, Any]:
        """Validate AI/ML dependencies."""
        if 'ai' in self.validation_results:
            return self.validation_results['ai']
        
        required_packages = {
            'openai': {
                'name': 'OpenAI',
                'purpose': 'LLM integration',
                'critical': True
            },
            'sentence_transformers': {
                'name': 'Sentence Transformers',
                'purpose': 'Text embeddings',
                'critical': False
            },
            'torch': {
                'name': 'PyTorch',
                'purpose': 'Deep learning',
                'critical': False
            }
        }
        
        result = self._validate_package_group(required_packages, 'AI analysis')
        self.validation_results['ai'] = result
        return result
    
    def _validate_package_group(self, packages: Dict[str, Dict], feature_name: str) -> Dict[str, Any]:
        """Validate a group of related packages."""
        available_packages = []
        missing_packages = []
        critical_missing = []
        
        for package_name, package_info in packages.items():
            try:
                module = importlib.import_module(package_name)
                version = getattr(module, '__version__', 'unknown')
                available_packages.append({
                    'name': package_info['name'],
                    'package': package_name,
                    'version': version,
                    'purpose': package_info['purpose']
                })
                logger.info(f"✓ {package_info['name']} {version} available")
                
            except ImportError as e:
                missing_info = {
                    'name': package_info['name'],
                    'package': package_name,
                    'purpose': package_info['purpose'],
                    'critical': package_info['critical'],
                    'error': str(e)
                }
                missing_packages.append(missing_info)
                
                if package_info['critical']:
                    critical_missing.append(missing_info)
                
                logger.warning(f"✗ {package_info['name']} not available: {e}")
        
        # Determine overall status
        if critical_missing:
            status = 'error'
            message = f"Critical dependencies missing for {feature_name}"
            user_message = f"{feature_name.title()} is not available due to missing required libraries."
        elif missing_packages:
            status = 'warning'
            message = f"Some optional dependencies missing for {feature_name}"
            user_message = f"{feature_name.title()} is available but some optional features may be limited."
        else:
            status = 'success'
            message = f"All dependencies available for {feature_name}"
            user_message = f"{feature_name.title()} is fully available."
        
        result = {
            'status': status,
            'message': message,
            'user_message': user_message,
            'available_packages': available_packages,
            'missing_packages': missing_packages,
            'critical_missing': critical_missing
        }
        
        # Add installation suggestions if there are missing packages
        if missing_packages:
            result['suggestions'] = self._get_installation_suggestions(
                [pkg['package'] for pkg in missing_packages]
            )
        
        return result
    
    def _get_installation_suggestions(self, package_names: List[str]) -> Dict[str, str]:
        """Get installation suggestions for missing packages."""
        suggestions = {
            'pip': f"pip install {' '.join(package_names)}",
            'conda': f"conda install {' '.join(package_names)}",
            'virtual_env': "Ensure you're running in the correct virtual environment with: source chatmrpt_venv/Scripts/activate"
        }
        
        # Add specific suggestions for common packages
        if 'geopandas' in package_names:
            suggestions['geopandas_specific'] = "For GeoPandas: conda install geopandas (recommended) or pip install geopandas"
        
        return suggestions
    
    def get_system_info(self) -> Dict[str, Any]:
        """Get system information for debugging."""
        return {
            'python_version': sys.version,
            'python_executable': sys.executable,
            'platform': sys.platform,
            'virtual_env': self._detect_virtual_env(),
            'environment_variables': {
                'VIRTUAL_ENV': os.environ.get('VIRTUAL_ENV'),
                'CONDA_DEFAULT_ENV': os.environ.get('CONDA_DEFAULT_ENV'),
                'PATH': os.environ.get('PATH', '')[:200] + '...'  # Truncated PATH
            }
        }
    
    def _detect_virtual_env(self) -> Dict[str, Any]:
        """Detect if running in a virtual environment."""
        venv_info = {
            'active': False,
            'type': None,
            'path': None
        }
        
        # Check for virtual environment
        if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
            venv_info['active'] = True
            venv_info['type'] = 'virtualenv'
            venv_info['path'] = sys.prefix
        
        # Check for conda environment
        if 'CONDA_DEFAULT_ENV' in os.environ:
            venv_info['active'] = True
            venv_info['type'] = 'conda'
            venv_info['path'] = os.environ.get('CONDA_PREFIX')
        
        return venv_info
    
    def clear_cache(self):
        """Clear dependency validation cache."""
        self.dependency_cache.clear()
        self.validation_results.clear()
        logger.info("Dependency validation cache cleared")

# Global validator instance
validator = DependencyValidator()

def require_dependencies(dependency_types: List[str], fallback_message: str = None):
    """
    Decorator to validate dependencies before executing a function.
    
    Args:
        dependency_types: List of dependency types to validate ('geospatial', 'visualization', 'settlement', 'ai')
        fallback_message: Custom message to return if dependencies are missing
    
    Returns:
        Decorator function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Validate all requested dependency types
            for dep_type in dependency_types:
                if dep_type == 'geospatial':
                    result = validator.validate_geospatial_dependencies()
                elif dep_type == 'visualization':
                    result = validator.validate_visualization_dependencies()
                elif dep_type == 'settlement':
                    result = validator.validate_settlement_dependencies()
                elif dep_type == 'ai':
                    result = validator.validate_ai_dependencies()
                else:
                    logger.warning(f"Unknown dependency type: {dep_type}")
                    continue
                
                if result['status'] == 'error':
                    return {
                        'status': 'error',
                        'message': result['message'],
                        'ai_response': fallback_message or result.get('user_message', 
                                     f"This feature requires dependencies that are not available: {dep_type}"),
                        'dependency_info': result
                    }
            
            # All dependencies validated, execute the function
            try:
                return func(*args, **kwargs)
            except ImportError as e:
                # Catch any import errors that slip through
                logger.error(f"Import error in {func.__name__}: {e}")
                return {
                    'status': 'error',
                    'message': f"Dependency error in {func.__name__}: {str(e)}",
                    'ai_response': fallback_message or f"A required library is not available for this operation."
                }
        
        return wrapper
    return decorator

# Convenience functions for common validations
def validate_for_settlement_analysis() -> Tuple[bool, Dict[str, Any]]:
    """Quick validation for settlement analysis features."""
    result = validator.validate_settlement_dependencies()
    return result['status'] == 'success', result

def validate_for_geospatial_analysis() -> Tuple[bool, Dict[str, Any]]:
    """Quick validation for geospatial analysis features."""
    result = validator.validate_geospatial_dependencies()
    return result['status'] == 'success', result

def get_dependency_status_summary() -> Dict[str, Any]:
    """Get a summary of all dependency statuses."""
    return {
        'geospatial': validator.validate_geospatial_dependencies(),
        'visualization': validator.validate_visualization_dependencies(),
        'settlement': validator.validate_settlement_dependencies(),
        'ai': validator.validate_ai_dependencies(),
        'system_info': validator.get_system_info()
    }