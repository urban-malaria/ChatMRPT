# app/models/analysis.py
"""
REFACTORED: Analysis functionality has been moved to app.analysis package

This file serves as a compatibility layer. All analysis functionality
is now available through the modular app.analysis package.

For new code, import directly from:
- app.analysis.core
- app.analysis.normalization
- app.analysis.composite
- app.analysis.vulnerability
- app.analysis.clustering
- app.analysis.correlation
- app.analysis.missing_data

Legacy Support: This file re-exports all functions for backward compatibility.
"""

# Import all functions from the new modular analysis package
from app.analysis import *

# Legacy compatibility - ensure all original function names are available
from app.analysis import (
    # Core functions
    normalize_variable,
    create_composite_risk_score,
    
    # Normalization functions
    normalize_data,
    apply_normalization,
    
    # Composite score functions
    create_composite_scores,
    calculate_composite_score,
    
    # Vulnerability analysis
    analyze_vulnerability,
    vulnerability_analysis,
    
    # Clustering functions
    perform_clustering_analysis,
    cluster_analysis,
    
    # Correlation functions
    correlation_analysis,
    calculate_correlations,
    
    # Missing data functions
    handle_missing_data,
    imputation_analysis
)

# Log the compatibility layer usage
import logging
logger = logging.getLogger(__name__)
logger.info("COMPATIBILITY: Using refactored analysis package through compatibility layer")

__all__ = [
    # Re-export everything from analysis package
    'normalize_variable',
    'create_composite_risk_score',
    'normalize_data',
    'apply_normalization',
    'create_composite_scores',
    'calculate_composite_score',
    'analyze_vulnerability',
    'vulnerability_analysis',
    'perform_clustering_analysis',
    'cluster_analysis',
    'correlation_analysis',
    'calculate_correlations',
    'handle_missing_data',
    'imputation_analysis'
] 