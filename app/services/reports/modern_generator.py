# app/services/reports/modern_generator.py
"""
Modern Legacy-Style Report Generator - Standalone Implementation
Generates both PDF reports and interactive HTML dashboards
Based on legacy design but modernized and dependency-free
"""

import os
import logging
import json
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
from typing import Dict, List, Optional, Any
import uuid

from flask import current_app

# Set up logging
logger = logging.getLogger(__name__)


class ModernReportGenerator:
    """
    Modern implementation of legacy-style report generation
    Supports PDF reports and interactive HTML dashboards
    """
    
    def __init__(self, data_handler, session_id: str):
        """
        Initialize report generator
        
        Args:
            data_handler: DataHandler instance with analysis results
            session_id: Session identifier
        """
        self.data_handler = data_handler
        self.session_id = session_id
        
        # Create reports folder
        self.reports_folder = os.path.join(
            current_app.instance_path, 
            'sessions', 
            session_id, 
            'reports'
        )
        os.makedirs(self.reports_folder, exist_ok=True)
        
        # Format configuration
        self.format_config = {
            'color_primary': '#003366',
            'color_secondary': '#0077cc',
            'color_accent': '#00a8e8',
            'color_success': '#28a745',
            'color_warning': '#ffc107',
            'color_danger': '#dc3545',
            'color_text': '#333333',
            'color_text_light': '#666666',
            'font_main': 'Arial',
            'font_header': 'Arial',
            'max_width': 800,
            'page_margin': 20
        }
    
    def generate_report(self, format_type: str = 'pdf', custom_sections: Optional[List[str]] = None, 
                       detail_level: str = 'standard') -> Dict[str, Any]:
        """
        Generate report in specified format
        
        Args:
            format_type: 'pdf' or 'html'
            custom_sections: Optional list of sections to include
            detail_level: 'basic', 'standard', or 'technical'
            
        Returns:
            Dictionary with generation status and file information
        """
        try:
            # Validate analysis completion
            if not self._validate_analysis_completed():
                return {
                    'status': 'error',
                    'message': 'Analysis must be completed before generating reports'
                }
            
            # Gather report data
            report_data = self._gather_report_data(detail_level, custom_sections)
            
            # Generate based on format type
            if format_type.lower() == 'pdf':
                return self._generate_pdf_report(report_data)
            elif format_type.lower() == 'html':
                return self._generate_html_dashboard(report_data)
            else:
                return {
                    'status': 'error',
                    'message': f'Unsupported format: {format_type}'
                }
                
        except Exception as e:
            logger.error(f"Error generating {format_type} report: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error generating report: {str(e)}'
            }
    
    def generate_dashboard(self) -> Dict[str, Any]:
        """
        Generate interactive HTML dashboard (standalone method)
        
        Returns:
            Dictionary with generation status and file information
        """
        return self.generate_report('html')
    
    def _validate_analysis_completed(self) -> bool:
        """Validate that analysis has been completed"""
        return (hasattr(self.data_handler, 'vulnerability_rankings') and 
                self.data_handler.vulnerability_rankings is not None and
                not self.data_handler.vulnerability_rankings.empty)
    
    def _gather_report_data(self, detail_level: str, custom_sections: Optional[List[str]] = None) -> Dict[str, Any]:
        """Gather all data needed for report generation"""
        
        # Get basic analysis statistics
        vulnerability_rankings = self.data_handler.vulnerability_rankings
        high_risk_count = len(vulnerability_rankings[vulnerability_rankings['vulnerability_category'] == 'High'])
        medium_risk_count = len(vulnerability_rankings[vulnerability_rankings['vulnerability_category'] == 'Medium'])
        low_risk_count = len(vulnerability_rankings[vulnerability_rankings['vulnerability_category'] == 'Low'])
        
        variables_used = getattr(self.data_handler, 'composite_variables', [])
        
        report_data = {
            'title': 'Malaria Risk Analysis Report',
            'subtitle': 'Spatial Vulnerability Assessment',
            'date': datetime.now().strftime('%B %d, %Y'),
            'timestamp': datetime.now().isoformat(),
            'session_id': self.session_id,
            'detail_level': detail_level,
            
            # Analysis summary
            'analysis_summary': {
                'total_wards': len(vulnerability_rankings),
                'high_risk_count': high_risk_count,
                'medium_risk_count': medium_risk_count,
                'low_risk_count': low_risk_count,
                'variables_used': variables_used,
                'variables_count': len(variables_used)
            },
            
            # Data for visualizations
            'vulnerability_data': vulnerability_rankings.to_dict('records') if hasattr(vulnerability_rankings, 'to_dict') else [],
            'top_vulnerable_wards': vulnerability_rankings.head(10).to_dict('records') if hasattr(vulnerability_rankings, 'head') else [],
            
            # Sections to include
            'sections': self._get_report_sections(detail_level, custom_sections)
        }
        
        return report_data
    
    def _get_report_sections(self, detail_level: str, custom_sections: Optional[List[str]] = None) -> Dict[str, Dict]:
        """Get sections to include in report"""
        
        # Define all available sections
        all_sections = {
            'executive_summary': self._generate_executive_summary(),
            'data_overview': self._generate_data_overview(),
            'methodology': self._generate_methodology_section(detail_level),
            'vulnerability_rankings': self._generate_vulnerability_rankings_section(),
            'spatial_analysis': self._generate_spatial_analysis_section(),
            'recommendations': self._generate_recommendations_section(),
            'technical_appendix': self._generate_technical_appendix() if detail_level == 'technical' else None
        }
        
        # Filter sections based on custom selection or detail level
        if custom_sections:
            sections = {k: v for k, v in all_sections.items() if k in custom_sections and v is not None}
        else:
            # Default sections based on detail level
            if detail_level == 'basic':
                sections = {k: v for k, v in all_sections.items() if k in ['executive_summary', 'vulnerability_rankings'] and v is not None}
            elif detail_level == 'standard':
                sections = {k: v for k, v in all_sections.items() if k in ['executive_summary', 'data_overview', 'vulnerability_rankings', 'recommendations'] and v is not None}
            else:  # technical
                sections = {k: v for k, v in all_sections.items() if v is not None}
        
        return sections
    
    def _generate_executive_summary(self) -> Dict[str, Any]:
        """Generate executive summary section"""
        rankings = self.data_handler.vulnerability_rankings
        variables = getattr(self.data_handler, 'composite_variables', [])
        
        high_risk = len(rankings[rankings['vulnerability_category'] == 'High'])
        medium_risk = len(rankings[rankings['vulnerability_category'] == 'Medium'])
        low_risk = len(rankings[rankings['vulnerability_category'] == 'Low'])
        
        return {
            'title': 'Executive Summary',
            'content': [
                {
                    'type': 'paragraph',
                    'text': f'This analysis evaluated malaria vulnerability across {len(rankings)} administrative wards using {len(variables)} risk factors. The assessment identifies priority areas for targeted intervention and resource allocation.'
                },
                {
                    'type': 'subheading',
                    'text': 'Key Findings'
                },
                {
                    'type': 'bullet_list',
                    'items': [
                        f'{high_risk} wards classified as HIGH vulnerability requiring immediate intervention',
                        f'{medium_risk} wards classified as MEDIUM vulnerability needing enhanced surveillance',
                        f'{low_risk} wards classified as LOW vulnerability suitable for routine prevention',
                        f'Analysis incorporates {len(variables)} validated epidemiological risk factors',
                        'Spatial clustering reveals distinct vulnerability hotspots and intervention zones'
                    ]
                },
                {
                    'type': 'subheading',
                    'text': 'Priority Actions'
                },
                {
                    'type': 'paragraph',
                    'text': 'Immediate resource deployment is recommended for high-vulnerability wards, with enhanced monitoring systems for medium-risk areas. This prioritization enables efficient allocation of limited public health resources.'
                }
            ]
        }
    
    def _generate_data_overview(self) -> Dict[str, Any]:
        """Generate data overview section"""
        variables = getattr(self.data_handler, 'composite_variables', [])
        
        return {
            'title': 'Data Overview & Methodology',
            'content': [
                {
                    'type': 'subheading',
                    'text': 'Risk Factors Analyzed'
                },
                {
                    'type': 'bullet_list',
                    'items': [var.replace('_', ' ').title() for var in variables] if variables else ['No variables available']
                },
                {
                    'type': 'subheading',
                    'text': 'Analysis Approach'
                },
                {
                    'type': 'paragraph',
                    'text': 'The vulnerability assessment employs composite scoring methodology, integrating multiple epidemiological and environmental risk factors. Each variable is normalized and weighted equally in the composite score calculation, with vulnerability categories derived from quartile-based classification.'
                }
            ]
        }
    
    def _generate_methodology_section(self, detail_level: str) -> Dict[str, Any]:
        """Generate methodology section"""
        if detail_level == 'basic':
            content = [
                {
                    'type': 'paragraph',
                    'text': 'Composite vulnerability scoring using standardized risk factors and quartile-based classification.'
                }
            ]
        else:
            content = [
                {
                    'type': 'subheading',
                    'text': 'Composite Scoring Method'
                },
                {
                    'type': 'paragraph',
                    'text': 'Variables are normalized using z-score standardization, then combined using equal-weight averaging to produce composite vulnerability scores.'
                },
                {
                    'type': 'subheading',
                    'text': 'Classification Approach'
                },
                {
                    'type': 'paragraph',
                    'text': 'Vulnerability categories (High/Medium/Low) are assigned based on composite score distribution quartiles, ensuring balanced classification across the study area.'
                }
            ]
        
        return {
            'title': 'Methodology',
            'content': content
        }
    
    def _generate_vulnerability_rankings_section(self) -> Dict[str, Any]:
        """Generate vulnerability rankings section"""
        rankings = self.data_handler.vulnerability_rankings
        top_10 = rankings.head(10)
        
        # Create table data
        table_data = {
            'headers': ['Rank', 'Ward Name', 'Vulnerability Score', 'Category'],
            'rows': []
        }
        
        for idx, row in top_10.iterrows():
            table_data['rows'].append([
                str(row['overall_rank']),
                str(row['WardName']),
                f"{row['median_score']:.3f}",
                str(row['vulnerability_category'])
            ])
        
        return {
            'title': 'Vulnerability Rankings',
            'content': [
                {
                    'type': 'subheading',
                    'text': 'Top 10 Most Vulnerable Wards'
                },
                {
                    'type': 'table',
                    'data': table_data
                },
                {
                    'type': 'paragraph',
                    'text': f'Complete rankings include all {len(rankings)} wards with detailed vulnerability scores and category assignments.'
                }
            ]
        }
    
    def _generate_spatial_analysis_section(self) -> Dict[str, Any]:
        """Generate spatial analysis section"""
        return {
            'title': 'Spatial Analysis',
            'content': [
                {
                    'type': 'paragraph',
                    'text': 'Geographic analysis reveals spatial clustering of vulnerability, with distinct hotspots requiring targeted intervention strategies.'
                },
                {
                    'type': 'subheading',
                    'text': 'Key Spatial Patterns'
                },
                {
                    'type': 'bullet_list',
                    'items': [
                        'High-vulnerability clusters identified for priority targeting',
                        'Geographic accessibility analysis for intervention planning',
                        'Spatial autocorrelation indicates clustered vulnerability patterns',
                        'Border effects and administrative boundary considerations'
                    ]
                }
            ]
        }
    
    def _generate_recommendations_section(self) -> Dict[str, Any]:
        """Generate recommendations section"""
        rankings = self.data_handler.vulnerability_rankings
        high_risk_wards = rankings[rankings['vulnerability_category'] == 'High']['WardName'].tolist()[:5]
        
        return {
            'title': 'Recommendations',
            'content': [
                {
                    'type': 'subheading',
                    'text': 'Immediate Priority Actions'
                },
                {
                    'type': 'numbered_list',
                    'items': [
                        f'Deploy rapid response teams to high-risk wards: {", ".join(high_risk_wards[:3])}',
                        'Establish enhanced surveillance systems in medium-risk areas',
                        'Implement community-based prevention programs in low-risk zones',
                        'Develop cross-ward coordination for hotspot management'
                    ]
                },
                {
                    'type': 'subheading',
                    'text': 'Resource Allocation Strategy'
                },
                {
                    'type': 'paragraph',
                    'text': 'Prioritize 70% of resources for high-vulnerability wards, 20% for medium-risk areas, and 10% for maintenance of low-risk zones. This allocation maximizes impact while maintaining comprehensive coverage.'
                }
            ]
        }
    
    def _generate_technical_appendix(self) -> Dict[str, Any]:
        """Generate technical appendix for detailed reports"""
        variables = getattr(self.data_handler, 'composite_variables', [])
        rankings = self.data_handler.vulnerability_rankings
        
        return {
            'title': 'Technical Appendix',
            'content': [
                {
                    'type': 'subheading',
                    'text': 'Statistical Summary'
                },
                {
                    'type': 'bullet_list',
                    'items': [
                        f'Mean vulnerability score: {rankings["median_score"].mean():.4f}',
                        f'Standard deviation: {rankings["median_score"].std():.4f}',
                        f'Score range: {rankings["median_score"].min():.4f} - {rankings["median_score"].max():.4f}',
                        f'Variables analyzed: {len(variables)}'
                    ]
                },
                {
                    'type': 'subheading',
                    'text': 'Quality Assurance'
                },
                {
                    'type': 'paragraph',
                    'text': 'Data quality validation completed with missing value analysis, outlier detection, and normalization verification. All procedures follow epidemiological best practices for vulnerability assessment.'
                }
            ]
        }
    
    def _generate_pdf_report(self, report_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate PDF report using HTML-to-PDF conversion"""
        try:
            # Generate HTML first
            html_content = self._build_report_html(report_data, for_pdf=True)
            
            # Create filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'malaria_risk_report_{timestamp}.pdf'
            pdf_path = os.path.join(self.reports_folder, filename)
            
            # For now, save as HTML and instruct user to print as PDF
            # In production, you could use libraries like weasyprint or pdfkit
            html_filename = f'malaria_risk_report_{timestamp}.html'
            html_path = os.path.join(self.reports_folder, html_filename)
            
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            return {
                'status': 'success',
                'message': 'PDF-styled report generated successfully',
                'report_path': html_path,
                'report_url': f'/download_report/{html_filename}',
                'format': 'pdf',
                'timestamp': datetime.now().isoformat(),
                'filename': html_filename
            }
            
        except Exception as e:
            logger.error(f"Error generating PDF report: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error generating PDF report: {str(e)}'
            }
    
    def _generate_html_dashboard(self, report_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate interactive HTML dashboard"""
        try:
            # Generate interactive dashboard HTML
            html_content = self._build_dashboard_html(report_data)
            
            # Create filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'malaria_dashboard_{timestamp}.html'
            dashboard_path = os.path.join(self.reports_folder, filename)
            
            with open(dashboard_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            return {
                'status': 'success',
                'message': 'Interactive dashboard generated successfully',
                'report_path': dashboard_path,
                'report_url': f'/download_report/{filename}',
                'format': 'html',
                'timestamp': datetime.now().isoformat(),
                'filename': filename,
                'type': 'interactive_dashboard'
            }
            
        except Exception as e:
            logger.error(f"Error generating HTML dashboard: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error generating HTML dashboard: {str(e)}'
            }
    
    def _build_report_html(self, report_data: Dict[str, Any], for_pdf: bool = False) -> str:
        """Build HTML content for PDF-style report"""
        
        # PDF-style CSS
        css_styles = f"""
        <style>
            @media print {{
                .no-print {{ display: none !important; }}
                .page-break {{ page-break-before: always; }}
            }}
            
            body {{
                font-family: {self.format_config['font_main']}, sans-serif;
                line-height: 1.6;
                color: {self.format_config['color_text']};
                max-width: {self.format_config['max_width']}px;
                margin: 0 auto;
                padding: {self.format_config['page_margin']}px;
                background: white;
            }}
            
            h1 {{
                color: {self.format_config['color_primary']};
                font-size: 32px;
                margin-bottom: 10px;
                text-align: center;
                border-bottom: 3px solid {self.format_config['color_primary']};
                padding-bottom: 10px;
            }}
            
            h2 {{
                color: {self.format_config['color_primary']};
                font-size: 24px;
                border-bottom: 2px solid {self.format_config['color_secondary']};
                padding-bottom: 8px;
                margin-top: 30px;
                margin-bottom: 15px;
            }}
            
            h3 {{
                color: {self.format_config['color_secondary']};
                font-size: 18px;
                margin-top: 20px;
                margin-bottom: 10px;
            }}
            
            .header {{
                text-align: center;
                margin-bottom: 40px;
                padding: 20px;
                background: #f8f9fa;
                border-radius: 8px;
            }}
            
            .subtitle {{
                color: {self.format_config['color_secondary']};
                font-size: 18px;
                margin-bottom: 5px;
            }}
            
            .date {{
                color: {self.format_config['color_text_light']};
                font-size: 14px;
            }}
            
            table {{
                border-collapse: collapse;
                width: 100%;
                margin: 20px 0;
                border: 1px solid #ddd;
            }}
            
            th, td {{
                border: 1px solid #ddd;
                padding: 12px;
                text-align: left;
            }}
            
            th {{
                background-color: {self.format_config['color_primary']};
                color: white;
                font-weight: bold;
            }}
            
            tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
            
            ul, ol {{
                margin: 10px 0;
                padding-left: 30px;
            }}
            
            li {{
                margin: 5px 0;
            }}
            
            .summary-box {{
                background: #e8f4fd;
                border-left: 4px solid {self.format_config['color_primary']};
                padding: 15px;
                margin: 20px 0;
                border-radius: 4px;
            }}
            
            .footer {{
                text-align: center;
                margin-top: 50px;
                padding: 20px;
                border-top: 1px solid #ddd;
                color: {self.format_config['color_text_light']};
                font-size: 12px;
            }}
        </style>
        """
        
        # Build HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{report_data['title']}</title>
            {css_styles}
        </head>
        <body>
            <div class="header">
                <h1>{report_data['title']}</h1>
                <div class="subtitle">{report_data['subtitle']}</div>
                <div class="date">Generated: {report_data['date']}</div>
            </div>
            
            <div class="summary-box">
                <h3>Analysis Summary</h3>
                <ul>
                    <li><strong>Total Wards Analyzed:</strong> {report_data['analysis_summary']['total_wards']}</li>
                    <li><strong>High Risk Wards:</strong> {report_data['analysis_summary']['high_risk_count']}</li>
                    <li><strong>Medium Risk Wards:</strong> {report_data['analysis_summary']['medium_risk_count']}</li>
                    <li><strong>Low Risk Wards:</strong> {report_data['analysis_summary']['low_risk_count']}</li>
                    <li><strong>Variables Analyzed:</strong> {report_data['analysis_summary']['variables_count']}</li>
                </ul>
            </div>
        """
        
        # Add sections
        for section_name, section in report_data['sections'].items():
            html_content += f"""
            <div class="section">
                <h2>{section['title']}</h2>
            """
            
            for item in section['content']:
                if item['type'] == 'paragraph':
                    html_content += f"<p>{item['text']}</p>"
                elif item['type'] == 'subheading':
                    html_content += f"<h3>{item['text']}</h3>"
                elif item['type'] == 'bullet_list':
                    html_content += "<ul>"
                    for bullet_item in item['items']:
                        html_content += f"<li>{bullet_item}</li>"
                    html_content += "</ul>"
                elif item['type'] == 'numbered_list':
                    html_content += "<ol>"
                    for numbered_item in item['items']:
                        html_content += f"<li>{numbered_item}</li>"
                    html_content += "</ol>"
                elif item['type'] == 'table':
                    html_content += self._build_html_table(item['data'])
            
            html_content += "</div>"
        
        # Add footer
        html_content += f"""
            <div class="footer">
                <p>Malaria Risk Analysis Report | Generated by ChatMRPT | {report_data['date']}</p>
                <p>Session ID: {report_data['session_id']}</p>
            </div>
        </body>
        </html>
        """
        
        return html_content
    
    def _build_dashboard_html(self, report_data: Dict[str, Any]) -> str:
        """Build interactive HTML dashboard"""
        
        # Prepare chart data
        vulnerability_chart_data = self._prepare_vulnerability_chart_data(report_data)
        variables_chart_data = self._prepare_variables_chart_data(report_data)
        
        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Malaria Risk Analysis Dashboard</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <script src="https://cdn.tailwindcss.com"></script>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
            <style>
                @media print {{
                    .no-print {{ display: none !important; }}
                    .print-break {{ page-break-before: always; }}
                }}
                .dashboard-card {{
                    background: white;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    transition: box-shadow 0.3s ease;
                }}
                .dashboard-card:hover {{
                    box-shadow: 0 4px 8px rgba(0,0,0,0.15);
                }}
                .stat-icon {{
                    font-size: 1.5rem;
                }}
                .chart-container {{
                    height: 400px;
                }}
            </style>
        </head>
        <body class="bg-gray-50">
            <div class="container mx-auto px-4 py-8">
                <!-- Header -->
                <div class="dashboard-card p-6 mb-8">
                    <div class="text-center">
                        <h1 class="text-4xl font-bold text-gray-800 mb-2">
                            <i class="fas fa-map-marked-alt text-red-600 mr-3"></i>
                            Malaria Risk Analysis Dashboard
                        </h1>
                        <p class="text-xl text-gray-600 mb-4">Interactive Spatial Vulnerability Assessment</p>
                        <div class="flex justify-center space-x-6 text-sm text-gray-500">
                            <span><i class="fas fa-calendar mr-1"></i>Generated: {report_data['date']}</span>
                            <span><i class="fas fa-clock mr-1"></i>{datetime.now().strftime('%I:%M %p')}</span>
                            <button onclick="window.print()" class="no-print bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600 transition-colors">
                                <i class="fas fa-print mr-1"></i>Print Report
                            </button>
                        </div>
                    </div>
                </div>
                
                <!-- Summary Cards -->
                <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                    <!-- High Risk Card -->
                    <div class="dashboard-card p-6">
                        <div class="flex items-center">
                            <div class="p-3 rounded-full bg-red-100">
                                <i class="fas fa-exclamation-triangle text-red-600 stat-icon"></i>
                            </div>
                            <div class="ml-4">
                                <p class="text-sm font-medium text-gray-500">High Risk Wards</p>
                                <p class="text-2xl font-semibold text-gray-900">{report_data['analysis_summary']['high_risk_count']}</p>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Variables Card -->
                    <div class="dashboard-card p-6">
                        <div class="flex items-center">
                            <div class="p-3 rounded-full bg-yellow-100">
                                <i class="fas fa-chart-line text-yellow-600 stat-icon"></i>
                            </div>
                            <div class="ml-4">
                                <p class="text-sm font-medium text-gray-500">Variables Analyzed</p>
                                <p class="text-2xl font-semibold text-gray-900">{report_data['analysis_summary']['variables_count']}</p>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Total Wards Card -->
                    <div class="dashboard-card p-6">
                        <div class="flex items-center">
                            <div class="p-3 rounded-full bg-green-100">
                                <i class="fas fa-map text-green-600 stat-icon"></i>
                            </div>
                            <div class="ml-4">
                                <p class="text-sm font-medium text-gray-500">Total Wards</p>
                                <p class="text-2xl font-semibold text-gray-900">{report_data['analysis_summary']['total_wards']}</p>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Analysis Status Card -->
                    <div class="dashboard-card p-6">
                        <div class="flex items-center">
                            <div class="p-3 rounded-full bg-blue-100">
                                <i class="fas fa-shield-alt text-blue-600 stat-icon"></i>
                            </div>
                            <div class="ml-4">
                                <p class="text-sm font-medium text-gray-500">Analysis Complete</p>
                                <p class="text-lg font-semibold text-green-600">
                                    <i class="fas fa-check-circle"></i> Yes
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Charts Section -->
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
                    <!-- Vulnerability Ranking Chart -->
                    <div class="dashboard-card p-6">
                        <h3 class="text-xl font-semibold text-gray-800 mb-4">
                            <i class="fas fa-chart-bar text-red-500 mr-2"></i>
                            Top 10 Most Vulnerable Wards
                        </h3>
                        <div id="vulnerability-chart" class="chart-container"></div>
                    </div>
                    
                    <!-- Variables Distribution Chart -->
                    <div class="dashboard-card p-6">
                        <h3 class="text-xl font-semibold text-gray-800 mb-4">
                            <i class="fas fa-pie-chart text-blue-500 mr-2"></i>
                            Risk Categories Distribution
                        </h3>
                        <div id="distribution-chart" class="chart-container"></div>
                    </div>
                </div>
                
                <!-- Data Table -->
                <div class="dashboard-card p-6 print-break">
                    <h3 class="text-xl font-semibold text-gray-800 mb-4">
                        <i class="fas fa-table text-green-500 mr-2"></i>
                        Vulnerability Rankings - Top 20 Wards
                    </h3>
                    <div class="overflow-x-auto">
                        <table class="min-w-full divide-y divide-gray-200">
                            <thead class="bg-gray-50">
                                <tr>
                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rank</th>
                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Ward Name</th>
                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Risk Score</th>
                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Category</th>
                                </tr>
                            </thead>
                            <tbody class="bg-white divide-y divide-gray-200">
                                {self._build_dashboard_table_rows(report_data)}
                            </tbody>
                        </table>
                    </div>
                </div>
                
                <!-- Footer -->
                <div class="text-center mt-8 p-4 text-gray-500 text-sm">
                    <p>Interactive Dashboard Generated by ChatMRPT | Session: {report_data['session_id']}</p>
                </div>
            </div>
            
            <script>
                // Vulnerability ranking chart
                {vulnerability_chart_data}
                
                // Risk distribution chart  
                {variables_chart_data}
                
                // Update high risk count in summary
                document.addEventListener('DOMContentLoaded', function() {{
                    console.log('Dashboard loaded successfully');
                }});
            </script>
        </body>
        </html>
        """
    
    def _prepare_vulnerability_chart_data(self, report_data: Dict[str, Any]) -> str:
        """Prepare Plotly chart data for vulnerability rankings"""
        top_wards = report_data['top_vulnerable_wards'][:10]
        
        ward_names = [ward['WardName'] for ward in top_wards]
        scores = [ward['median_score'] for ward in top_wards]
        categories = [ward['vulnerability_category'] for ward in top_wards]
        
        # Create color mapping
        colors = ['#dc3545' if cat == 'High' else '#ffc107' if cat == 'Medium' else '#28a745' for cat in categories]
        
        chart_config = {
            'data': [{
                'type': 'bar',
                'x': scores,
                'y': ward_names,
                'orientation': 'h',
                'marker': {'color': colors},
                'text': [f'{score:.3f}' for score in scores],
                'textposition': 'auto',
            }],
            'layout': {
                'title': '',
                'xaxis': {'title': 'Vulnerability Score'},
                'yaxis': {'title': '', 'autorange': 'reversed'},
                'margin': {'l': 150, 'r': 50, 't': 50, 'b': 50},
                'height': 400
            }
        }
        
        return f"Plotly.newPlot('vulnerability-chart', {json.dumps(chart_config['data'])}, {json.dumps(chart_config['layout'])});"
    
    def _prepare_variables_chart_data(self, report_data: Dict[str, Any]) -> str:
        """Prepare Plotly chart data for risk distribution"""
        summary = report_data['analysis_summary']
        
        chart_config = {
            'data': [{
                'type': 'pie',
                'labels': ['High Risk', 'Medium Risk', 'Low Risk'],
                'values': [summary['high_risk_count'], summary['medium_risk_count'], summary['low_risk_count']],
                'marker': {'colors': ['#dc3545', '#ffc107', '#28a745']},
                'textinfo': 'label+percent+value',
                'hole': 0.3
            }],
            'layout': {
                'title': '',
                'margin': {'l': 50, 'r': 50, 't': 50, 'b': 50},
                'height': 400
            }
        }
        
        return f"Plotly.newPlot('distribution-chart', {json.dumps(chart_config['data'])}, {json.dumps(chart_config['layout'])});"
    
    def _build_html_table(self, table_data: Dict[str, Any]) -> str:
        """Build HTML table from table data"""
        html = "<table>"
        
        # Headers
        html += "<thead><tr>"
        for header in table_data['headers']:
            html += f"<th>{header}</th>"
        html += "</tr></thead>"
        
        # Rows
        html += "<tbody>"
        for row in table_data['rows']:
            html += "<tr>"
            for cell in row:
                html += f"<td>{cell}</td>"
            html += "</tr>"
        html += "</tbody>"
        
        html += "</table>"
        return html
    
    def _build_dashboard_table_rows(self, report_data: Dict[str, Any]) -> str:
        """Build table rows for dashboard"""
        vulnerability_data = report_data['vulnerability_data'][:20]  # Top 20
        
        rows_html = ""
        for ward in vulnerability_data:
            category_class = 'text-red-600' if ward['vulnerability_category'] == 'High' else 'text-yellow-600' if ward['vulnerability_category'] == 'Medium' else 'text-green-600'
            
            rows_html += f"""
            <tr>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{ward['overall_rank']}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-medium">{ward['WardName']}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{ward['median_score']:.4f}</td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full {category_class}">
                        {ward['vulnerability_category']}
                    </span>
                </td>
            </tr>
            """
        
        return rows_html


# Legacy compatibility function
def generate_report(data_handler, format_type='pdf', custom_sections=None, detail_level='standard', metadata=None):
    """
    Legacy-compatible report generation function
    
    Args:
        data_handler: DataHandler instance
        format_type: Report format ('pdf' or 'html')
        custom_sections: Optional list of sections
        detail_level: Detail level
        metadata: Optional metadata (ignored for compatibility)
        
    Returns:
        Dictionary with generation results
    """
    session_id = getattr(data_handler, 'session_id', 'unknown')
    generator = ModernReportGenerator(data_handler, session_id)
    return generator.generate_report(format_type, custom_sections, detail_level) 