"""
Report generation service for ChatMRPT.

This service provides functionality for generating analysis reports
in various formats with legacy-style quality but modern implementation.
"""

import logging
import os
from typing import Dict, Any, Optional, List
from datetime import datetime
from flask import send_from_directory, current_app

logger = logging.getLogger(__name__)


class ReportService:
    """
    Modern report service that generates both PDF reports and interactive HTML dashboards.
    
    Replaces the legacy report generation system with improved implementation
    while maintaining the same high-quality output users expect.
    """
    
    def __init__(self, reports_folder: str, llm_manager=None):
        """
        Initialize the report service.
        
        Args:
            reports_folder: Path to reports output folder
            llm_manager: LLM manager for generating content (optional)
        """
        self.reports_folder = reports_folder
        self.llm_manager = llm_manager
        os.makedirs(reports_folder, exist_ok=True)
        
        logger.info(f"ReportService initialized with reports folder: {reports_folder}")
    
    def generate_report(self, data_handler, session_id: str, format_type: str = 'pdf', 
                       custom_sections: Optional[List[str]] = None, 
                       detail_level: str = 'standard') -> Dict[str, Any]:
        """
        Generate report using our modern legacy-style generator
        
        Args:
            data_handler: DataHandler with analysis results
            session_id: User session ID
            format_type: Output format ('pdf' for PDF-styled, 'html' for dashboard)
            custom_sections: Optional list of sections to include
            detail_level: Level of detail (basic, standard, technical)
            
        Returns:
            Dictionary with status and file information
        """
        try:
            logger.info(f"Generating {format_type} report for session {session_id}")
            
            # Validate analysis completion
            if not hasattr(data_handler, 'vulnerability_rankings') or data_handler.vulnerability_rankings is None:
                return {
                    'status': 'error',
                    'message': 'Analysis must be completed before generating reports. Please run analysis first.'
                }
            
            # Import and use our modern report generator
            from .modern_generator import ModernReportGenerator
            
            # Create generator instance
            generator = ModernReportGenerator(data_handler, session_id)
            
            # Generate report
            result = generator.generate_report(
                format_type=format_type,
                custom_sections=custom_sections,
                detail_level=detail_level
            )
            
            # Log result
            if result.get('status') == 'success':
                logger.info(f"Successfully generated {format_type} report: {result.get('filename')}")
            else:
                logger.error(f"Failed to generate {format_type} report: {result.get('message')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in report generation: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error generating report: {str(e)}'
            }
    
    def generate_analysis_report(self, session_id: str, data_handler, 
                               format_type: str = 'pdf', 
                               sections: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Generate a comprehensive analysis report (compatibility method).
        
        Args:
            session_id: User session ID
            data_handler: DataHandler with analysis results
            format_type: Output format ('pdf' or 'html')
            sections: Optional list of sections to include
            
        Returns:
            Dictionary with status and file information
        """
        return self.generate_report(
            data_handler=data_handler,
            session_id=session_id,
            format_type=format_type,
            custom_sections=sections,
            detail_level='standard'
        )
    
    def generate_dashboard(self, data_handler, session_id: str) -> Dict[str, Any]:
        """
        Generate interactive HTML dashboard.
        
        Args:
            data_handler: DataHandler with analysis results
            session_id: User session ID
            
        Returns:
            Dictionary with status and dashboard information
        """
        try:
            logger.info(f"Generating interactive dashboard for session {session_id}")
            
            # Use our modern generator for dashboard
            from .modern_generator import ModernReportGenerator
            
            generator = ModernReportGenerator(data_handler, session_id)
            result = generator.generate_dashboard()
            
            if result.get('status') == 'success':
                logger.info(f"Successfully generated dashboard: {result.get('filename')}")
            else:
                logger.error(f"Failed to generate dashboard: {result.get('message')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error generating dashboard: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error generating dashboard: {str(e)}'
            }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the report service.
        
        Returns:
            Dictionary with health status
        """
        try:
            # Check if reports folder is writable
            test_file = os.path.join(self.reports_folder, 'health_check.txt')
            with open(test_file, 'w') as f:
                f.write('Health check')
            os.remove(test_file)
            
            # Check if modern generator can be imported
            from .modern_generator import ModernReportGenerator
            
            return {
                'status': 'healthy',
                'message': 'Report service is operational',
                'reports_folder': self.reports_folder,
                'modern_generator_available': True,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Report service health check failed: {str(e)}")
            return {
                'status': 'unhealthy',
                'message': f'Health check failed: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }

    def serve_report_file(self, session_id: str, filename: str) -> Dict[str, Any]:
        """
        Serve a report file for download.
        
        Args:
            session_id: User session ID
            filename: Name of the report file to serve
            
        Returns:
            Dictionary with status and Flask response object
        """
        try:
            # Validate filename for security
            if not filename or '..' in filename or '/' in filename or '\\' in filename:
                return {
                    'status': 'error',
                    'message': 'Invalid filename'
                }
            
            # Use session-specific reports folder
            session_reports_folder = os.path.join(current_app.instance_path, 'sessions', session_id, 'reports')
            file_path = os.path.join(session_reports_folder, filename)
            
            if not os.path.exists(file_path):
                logger.error(f"Report file not found: {file_path}")
                return {
                    'status': 'error',
                    'message': 'Report file not found'
                }
            
            # Security check - ensure file is within reports folder
            safe_path = os.path.abspath(file_path)
            reports_abs_path = os.path.abspath(session_reports_folder)
            
            if not safe_path.startswith(reports_abs_path):
                logger.error(f"Attempt to access file outside reports folder: {filename}")
                return {
                    'status': 'error',
                    'message': 'Access denied'
                }
            
            # Log the download
            logger.info(f"Serving report file: {filename} for session {session_id}")
            
            # Return Flask response for file download
            response = send_from_directory(
                session_reports_folder, 
                filename, 
                as_attachment=True
            )
            
            return {
                'status': 'success',
                'response': response
            }
            
        except Exception as e:
            logger.error(f"Error serving report file {filename}: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error serving report file: {str(e)}'
            } 