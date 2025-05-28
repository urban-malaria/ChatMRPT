"""
Analysis module for ChatMRPT

This module provides refactored analysis functionality,
split from the monolithic analysis.py into focused modules.
"""

from .metadata import AnalysisMetadata
from .normalization import normalize_data, normalize_variable, determine_variable_relationships
from .utils import is_numeric_column, get_column_stats, check_data_quality
from .imputation import (
    handle_missing_values, 
    handle_spatial_imputation,
    handle_mean_imputation,
    handle_mode_imputation,
    get_imputation_summary,
    process_ward_for_spatial_imputation
)
from .scoring import (
    compute_composite_scores,
    compute_composite_score_model,
    analyze_vulnerability,
    get_scoring_summary,
    validate_scoring_inputs
)
from .urban_analysis import (
    analyze_urban_extent,
    get_urban_extent_summary,
    validate_urban_analysis_inputs,
    classify_urban_wards,
    get_urban_statistics
)
from .pipeline import (
    run_full_analysis_pipeline,
    get_explanation_for_visualization,
    get_explanation_for_ward,
    get_explanation_for_analysis_result,
    generate_analysis_report,
    generate_markdown_report,
    generate_html_report
)

__all__ = [
    'AnalysisMetadata',
    'normalize_data',
    'normalize_variable', 
    'determine_variable_relationships',
    'is_numeric_column',
    'get_column_stats',
    'check_data_quality',
    'handle_missing_values',
    'handle_spatial_imputation',
    'handle_mean_imputation',
    'handle_mode_imputation',
    'get_imputation_summary',
    'process_ward_for_spatial_imputation',
    'compute_composite_scores',
    'compute_composite_score_model',
    'analyze_vulnerability',
    'get_scoring_summary',
    'validate_scoring_inputs',
    'analyze_urban_extent',
    'get_urban_extent_summary',
    'validate_urban_analysis_inputs',
    'classify_urban_wards',
    'get_urban_statistics',
    'run_full_analysis_pipeline',
    'get_explanation_for_visualization',
    'get_explanation_for_ward',
    'get_explanation_for_analysis_result',
    'generate_analysis_report',
    'generate_markdown_report',
    'generate_html_report'
] 