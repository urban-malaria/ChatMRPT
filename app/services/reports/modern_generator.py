"""
Modern Report Generator for ChatMRPT

This module provides modern report generation functionality for malaria risk analysis.
Enhanced to support ITN distribution results export.
"""

import logging
import os
import json
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd
import geopandas as gpd

logger = logging.getLogger(__name__)


class ModernReportGenerator:
    """Modern report generator for ChatMRPT analysis results"""
    
    def __init__(self, data_handler, session_id: str):
        """
        Initialize the modern report generator.
        
        Args:
            data_handler: Data handler containing analysis results
            session_id: Session identifier
        """
        self.data_handler = data_handler
        self.session_id = session_id
        logger.info(f"ModernReportGenerator initialized for session {session_id}")
    
    def generate_report(self, format_type: str = 'pdf', custom_sections: Optional[List[str]] = None, 
                       detail_level: str = 'standard') -> Dict[str, Any]:
        """
        Generate a report based on analysis results.
        
        Args:
            format_type: Type of report ('pdf', 'html', 'json', 'export')
            custom_sections: Optional list of custom sections to include
            detail_level: Level of detail ('summary', 'standard', 'detailed')
            
        Returns:
            Dict containing report generation results
        """
        try:
            logger.info(f"Generating {format_type} report for session {self.session_id}")
            
            # Check if ITN distribution results exist
            itn_results_path = f"instance/uploads/{self.session_id}/itn_distribution_results.json"
            has_itn_results = os.path.exists(itn_results_path)
            
            if has_itn_results:
                logger.info("ITN distribution results found - generating comprehensive export")
                return self._generate_itn_export()
            
            # Check if basic analysis results exist
            if hasattr(self.data_handler, 'vulnerability_rankings') and self.data_handler.vulnerability_rankings is not None:
                logger.info("Basic analysis results found - generating analysis export")
                return self._generate_analysis_export()
            
            # Fallback to placeholder
            return {
                'status': 'success',
                'message': 'No analysis results found. Please complete an analysis first.',
                'format': format_type,
                'detail_level': detail_level,
                'session_id': self.session_id,
                'file_path': None,
                'sections_included': custom_sections or ['summary', 'analysis', 'recommendations']
            }
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return {
                'status': 'error',
                'message': f'Failed to generate {format_type} report: {str(e)}',
                'format': format_type,
                'session_id': self.session_id
            }
    
    def _generate_itn_export(self) -> Dict[str, Any]:
        """Generate comprehensive ITN distribution export package"""
        try:
            # Import the export tool functionality
            from app.planning.export_tools import ExportITNResults
            
            # Create export tool instance
            export_tool = ExportITNResults(
                include_dashboard=True,
                include_csv=True,
                include_maps=True
            )
            
            # Execute export
            result = export_tool.execute(self.session_id)
            
            if result.success:
                return {
                    'status': 'success',
                    'message': result.message,
                    'format': 'export',
                    'session_id': self.session_id,
                    'file_path': result.data.get('export_path'),
                    'web_path': result.data.get('web_path'),
                    'report_url': result.data.get('web_path'),  # For compatibility
                    'download_links': [{
                        'name': 'ITN Distribution Export Package',
                        'url': result.data.get('web_path'),
                        'type': 'zip',
                        'size': f"{result.data.get('package_size_mb', 0):.1f} MB"
                    }]
                }
            else:
                return {
                    'status': 'error',
                    'message': result.message,
                    'format': 'export',
                    'session_id': self.session_id
                }
                
        except Exception as e:
            logger.error(f"Error generating ITN export: {e}")
            return {
                'status': 'error',
                'message': f'Failed to generate ITN export: {str(e)}',
                'format': 'export',
                'session_id': self.session_id
            }
    
    def _generate_analysis_export(self) -> Dict[str, Any]:
        """Generate basic analysis export with vulnerability rankings"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_dir = Path(f"instance/exports/{self.session_id}/analysis_export_{timestamp}")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            # Export vulnerability rankings
            csv_path = export_dir / 'vulnerability_rankings.csv'
            
            if hasattr(self.data_handler, 'vulnerability_rankings') and self.data_handler.vulnerability_rankings is not None:
                self.data_handler.vulnerability_rankings.to_csv(csv_path, index=False)
            
            # Create a simple summary
            summary_path = export_dir / 'analysis_summary.txt'
            with open(summary_path, 'w') as f:
                f.write(f"Malaria Risk Analysis Summary\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"Session: {self.session_id}\n\n")
                
                if hasattr(self.data_handler, 'vulnerability_rankings'):
                    f.write(f"Total Wards Analyzed: {len(self.data_handler.vulnerability_rankings)}\n")
                    if 'composite_category' in self.data_handler.vulnerability_rankings.columns:
                        category_counts = self.data_handler.vulnerability_rankings['composite_category'].value_counts()
                        f.write("\nRisk Distribution:\n")
                        for category, count in category_counts.items():
                            f.write(f"- {category}: {count} wards\n")
            
            # Create ZIP
            zip_name = f"Analysis_Export_{timestamp}.zip"
            zip_path = export_dir.parent / zip_name
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in export_dir.iterdir():
                    if file_path.is_file():
                        zipf.write(file_path, file_path.name)
            
            # Calculate size
            package_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
            
            # Create download link
            web_path = f"/export/download/{self.session_id}/{zip_name}"
            
            return {
                'status': 'success',
                'message': 'Analysis export package created successfully!',
                'format': 'export',
                'session_id': self.session_id,
                'file_path': str(zip_path),
                'web_path': web_path,
                'report_url': web_path,  # For compatibility
                'download_links': [{
                    'name': 'Analysis Export Package',
                    'url': web_path,
                    'type': 'zip',
                    'size': f"{package_size_mb:.1f} MB"
                }]
            }
            
        except Exception as e:
            logger.error(f"Error generating analysis export: {e}")
            return {
                'status': 'error',
                'message': f'Failed to generate analysis export: {str(e)}',
                'format': 'export',
                'session_id': self.session_id
            }
    
    def _generate_pdf_report(self) -> str:
        """Generate PDF report (placeholder)"""
        # Placeholder for PDF generation
        return "pdf_report_placeholder.pdf"
    
    def _generate_html_report(self) -> str:
        """Generate HTML report (placeholder)"""
        # Placeholder for HTML generation
        return "html_report_placeholder.html"
    
    def _generate_json_report(self) -> Dict[str, Any]:
        """Generate JSON report (placeholder)"""
        # Placeholder for JSON generation
        return {"report": "json_report_placeholder"}
    
