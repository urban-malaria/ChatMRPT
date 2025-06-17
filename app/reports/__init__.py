"""
Reports package for ChatMRPT.

This package contains modern report generation functionality that creates
both PDF reports and interactive HTML dashboards based on legacy design
but with improved implementation.
"""

from ..services.reports.modern_generator import ModernReportGenerator

# Define the generate_report function that mirrors the class method
def generate_report(data_handler, format_type='pdf', custom_sections=None, detail_level='standard'):
    """Wrapper for ModernReportGenerator.generate_report"""
    session_id = getattr(data_handler, 'session_id', 'unknown')
    generator = ModernReportGenerator(data_handler, session_id)
    return generator.generate_report(format_type, custom_sections, detail_level)

__all__ = [
    'ModernReportGenerator',
    'generate_report'
] 