# app/models/report_generator.py
"""
REFACTORED: Report generation functionality has been moved to app.reports package

This file serves as a compatibility layer. All report generation functionality
is now available through the modular app.reports package.

For new code, import directly from:
- app.reports.core
- app.reports.generators
- app.reports.exporters
- app.reports.formatting
- app.reports.templates
- app.reports.utils
- app.reports.validators

Legacy Support: This file re-exports all functions for backward compatibility.
"""

# Import all functions from the new modular reports package
from app.reports import *

# Legacy compatibility - ensure all original function names are available
from app.reports import (
    # Core report functions
    generate_report,
    create_report,
    
    # Report generators
    generate_analysis_report,
    generate_visualization_report,
    generate_summary_report,
    
    # Export functions
    export_to_pdf,
    export_to_html,
    export_to_docx,
    
    # Formatting functions
    format_report_data,
    apply_report_styling,
    
    # Template functions
    load_report_template,
    render_template,
    
    # Utility functions
    validate_report_data,
    prepare_report_context,
    
    # Validator functions
    validate_report_structure,
    check_data_completeness
)

# Log the compatibility layer usage
import logging
logger = logging.getLogger(__name__)
logger.info("COMPATIBILITY: Using refactored reports package through compatibility layer")

__all__ = [
    # Re-export everything from reports package
    'generate_report',
    'create_report',
    'generate_analysis_report',
    'generate_visualization_report',
    'generate_summary_report',
    'export_to_pdf',
    'export_to_html',
    'export_to_docx',
    'format_report_data',
    'apply_report_styling',
    'load_report_template',
    'render_template',
    'validate_report_data',
    'prepare_report_context',
    'validate_report_structure',
    'check_data_completeness'
] 