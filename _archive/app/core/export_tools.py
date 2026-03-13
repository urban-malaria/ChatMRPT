"""
Export Tools for ChatMRPT - Modular Export System

This module provides export functionality for analysis results without
modifying any existing tools or pipelines. Completely standalone.
"""

import os
import json
import logging
import zipfile
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import pandas as pd
import geopandas as gpd
from pydantic import Field

from .base import BaseTool, ToolCategory, ToolExecutionResult, get_session_unified_dataset
from ..data.unified_dataset_builder import load_unified_dataset

logger = logging.getLogger(__name__)


class ExportITNResults(BaseTool):
    """
    Export ITN distribution results as a comprehensive take-home package.
    
    This tool creates a professional export package including:
    - Interactive dashboard (HTML)
    - Detailed results (CSV)
    - Maps and visualizations
    
    Completely modular - doesn't affect existing functionality.
    """
    
    include_dashboard: bool = Field(
        True,
        description="Include interactive HTML dashboard"
    )
    
    include_csv: bool = Field(
        True,
        description="Include detailed CSV with all rankings and allocations"
    )
    
    include_maps: bool = Field(
        True,
        description="Include standalone map files"
    )
    
    @classmethod
    def get_tool_name(cls) -> str:
        return "export_itn_results"
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.REPORT_GENERATION
    
    @classmethod
    def get_description(cls) -> str:
        return "Export comprehensive ITN distribution results package for stakeholder presentation"
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Export ITN distribution results",
            "Create take-home package for ITN analysis",
            "Generate ITN distribution report",
            "Export analysis results for presentation"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute ITN results export"""
        try:
            logger.info(f"Starting ITN results export for session {session_id}")
            
            # Create export directory
            export_dir = self._create_export_directory(session_id)
            
            # Gather all necessary data
            export_data = self._gather_export_data(session_id)
            if not export_data:
                return self._create_error_result(
                    "No ITN distribution results found. Please run ITN distribution planning first."
                )
            
            exported_files = []
            
            # 1. Export CSV if requested
            if self.include_csv:
                csv_path = self._export_csv(export_data, export_dir)
                if csv_path:
                    exported_files.append(csv_path)
                    logger.info(f"Exported CSV: {csv_path}")
            
            # 2. Generate dashboard if requested
            if self.include_dashboard:
                dashboard_path = self._generate_dashboard(export_data, export_dir, session_id)
                if dashboard_path:
                    exported_files.append(dashboard_path)
                    logger.info(f"Generated dashboard: {dashboard_path}")
            
            # 3. Copy maps if requested
            if self.include_maps:
                map_files = self._copy_maps(export_data, export_dir, session_id)
                exported_files.extend(map_files)
                logger.info(f"Copied {len(map_files)} map files")
            
            # 4. Create summary report instead of JSON metadata
            summary_path = self._create_summary_report(export_data, export_dir)
            if summary_path:
                exported_files.append(summary_path)
            
            # 5. Create ZIP package
            zip_path = self._create_zip_package(exported_files, export_dir, session_id)
            
            # Calculate package size
            package_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
            
            # Create web path for download
            web_path = f"/export/download/{session_id}/{os.path.basename(zip_path)}"
            
            # Prepare success message
            message = self._format_export_summary(export_data, len(exported_files), package_size_mb)
            
            result_data = {
                'export_path': zip_path,
                'web_path': web_path,
                'files_included': len(exported_files),
                'package_size_mb': round(package_size_mb, 2),
                'export_type': 'itn_distribution',
                'timestamp': datetime.now().isoformat()
            }
            
            return self._create_success_result(
                message=message,
                data=result_data,
                web_path=web_path
            )
            
        except Exception as e:
            logger.error(f"Error exporting ITN results: {e}", exc_info=True)
            return self._create_error_result(f"Export failed: {str(e)}")
    
    def _create_export_directory(self, session_id: str) -> Path:
        """Create directory for export files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_dir = Path(f"instance/exports/{session_id}/itn_export_{timestamp}")
        export_dir.mkdir(parents=True, exist_ok=True)
        return export_dir
    
    def _gather_export_data(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Gather all data needed for export"""
        try:
            # Load unified dataset with geometry
            gdf = load_unified_dataset(session_id, require_geometry=True)
            if gdf is None:
                logger.error("No unified dataset found")
                return None
            
            # Check for ITN distribution results
            itn_results_path = f"instance/uploads/{session_id}/itn_distribution_results.json"
            if not os.path.exists(itn_results_path):
                logger.error("No ITN distribution results found")
                return None
            
            with open(itn_results_path, 'r') as f:
                itn_results = json.load(f)
            
            # Gather visualization paths from session folder
            viz_dir = Path(f"instance/uploads/{session_id}")
            
            # Look for maps in the root of session folder
            vulnerability_maps = list(viz_dir.glob("*vulnerability*map*.html"))
            itn_maps = list(viz_dir.glob("*itn*distribution*map*.html"))
            
            # Also check in visualizations subfolder
            viz_subdir = viz_dir / "visualizations"
            if viz_subdir.exists():
                vulnerability_maps.extend(list(viz_subdir.glob("*vulnerability*map*.html")))
                itn_maps.extend(list(viz_subdir.glob("*itn*distribution*map*.html")))
            
            export_data = {
                'unified_dataset': gdf,
                'itn_results': itn_results,
                'vulnerability_maps': vulnerability_maps,
                'itn_maps': itn_maps,
                'session_id': session_id,
                'export_date': datetime.now(),
                'total_wards': len(gdf),
                'analysis_method': itn_results.get('method', 'composite')
            }
            
            # Add summary statistics
            if 'stats' in itn_results:
                export_data['summary_stats'] = itn_results['stats']
            
            return export_data
            
        except Exception as e:
            logger.error(f"Error gathering export data: {e}")
            return None
    
    def _export_csv(self, export_data: Dict[str, Any], export_dir: Path) -> Optional[Path]:
        """Export detailed CSV with all rankings and ITN allocations"""
        try:
            gdf = export_data['unified_dataset']
            itn_results = export_data['itn_results']
            
            # Get actual column names from the data
            # Handle different naming conventions
            ward_col = next((col for col in gdf.columns if col.lower() in ['wardname', 'ward_name', 'ward']), None)
            lga_col = next((col for col in gdf.columns if col.upper() == 'LGA' or col.lower() in ['lga_name', 'lganame']), None)
            
            # Select ALL relevant columns for comprehensive export
            export_columns = []
            
            # Always include ward and LGA if available
            if ward_col:
                export_columns.append(ward_col)
            if lga_col:
                export_columns.append(lga_col)
                
            # Add other important columns
            export_columns.extend([
                # Analysis results
                'composite_score', 'composite_rank', 'composite_category',
                'pca_score', 'pca_rank', 'pca_category',
                # Health indicators
                'TPR', 'U5_population',
                # Urban status
                'Urban', 'urbanPercentage',
                # Environmental factors
                'housing_quality', 'evi', 'ndwi', 'soil_wetness'
            ])
            
            # Now merge with ITN results to get allocation data
            if 'prioritized' in itn_results:
                # Create a dataframe from ITN results
                itn_df = pd.DataFrame(itn_results['prioritized'])
                if 'reprioritized' in itn_results and itn_results['reprioritized']:
                    repri_df = pd.DataFrame(itn_results['reprioritized'])
                    itn_df = pd.concat([itn_df, repri_df], ignore_index=True)
                
                # Merge ITN allocation data
                if ward_col and 'ward_name' in itn_df.columns:
                    gdf = gdf.merge(
                        itn_df[['ward_name', 'population', 'nets_allocated', 'nets_needed', 
                               'coverage_percent', 'allocation_phase']],
                        left_on=ward_col,
                        right_on='ward_name',
                        how='left'
                    )
                    # Add ITN columns to export
                    export_columns.extend(['population', 'nets_allocated', 'nets_needed', 
                                         'coverage_percent', 'allocation_phase'])
            
            # Filter to existing columns
            available_columns = [col for col in export_columns if col in gdf.columns]
            
            # Create export dataframe
            export_df = gdf[available_columns].copy()
            
            # Add ITN allocation data if available
            if 'prioritized' in itn_results and 'reprioritized' in itn_results:
                # This would merge the ITN allocation results
                # For now, we'll use what's in the unified dataset
                pass
            
            # Add calculated columns if not present
            if 'households' not in export_df.columns and 'population' in export_df.columns:
                # Estimate households based on average household size
                avg_household_size = 4  # Default, could be from config
                export_df['households'] = (export_df['population'] / avg_household_size).round().astype(int)
            
            if 'nets_needed' not in export_df.columns and 'households' in export_df.columns:
                # Calculate nets needed (2 nets per household standard)
                export_df['nets_needed'] = export_df['households'] * 2
            
            # Sort by composite rank
            if 'composite_rank' in export_df.columns:
                export_df = export_df.sort_values('composite_rank')
            
            # Save CSV
            csv_path = export_dir / 'itn_distribution_results.csv'
            export_df.to_csv(csv_path, index=False)
            
            return csv_path
            
        except Exception as e:
            logger.error(f"Error exporting CSV: {e}")
            return None
    
    def _generate_dashboard(self, export_data: Dict[str, Any], export_dir: Path, session_id: str) -> Optional[Path]:
        """Generate interactive HTML dashboard"""
        try:
            # For now, create a placeholder dashboard
            # In the next step, we'll create the actual template
            dashboard_html = self._create_dashboard_html(export_data)
            
            dashboard_path = export_dir / 'itn_distribution_dashboard.html'
            with open(dashboard_path, 'w', encoding='utf-8') as f:
                f.write(dashboard_html)
            
            return dashboard_path
            
        except Exception as e:
            logger.error(f"Error generating dashboard: {e}")
            return None
    
    def _create_dashboard_html(self, export_data: Dict[str, Any]) -> str:
        """Create comprehensive and creative dashboard HTML content"""
        stats = export_data.get('summary_stats', {})
        gdf = export_data['unified_dataset']
        itn_results = export_data.get('itn_results', {})
        
        # Calculate additional insights
        try:
            high_risk_count = len(gdf[gdf['composite_category'] == 'High Risk']) if 'composite_category' in gdf.columns else 0
            very_high_risk_count = len(gdf[gdf['composite_category'] == 'Very High Risk']) if 'composite_category' in gdf.columns else 0
        except Exception as e:
            logger.warning(f"Error calculating risk counts: {e}")
            high_risk_count = 0
            very_high_risk_count = 0
        
        # Get top 10 wards
        top_wards_html = ""
        try:
            if 'composite_rank' in gdf.columns and 'ward_name' in gdf.columns:
                # Select only the columns we need to avoid dict/geometry issues
                needed_cols = ['ward_name', 'lga_name', 'composite_score', 'composite_rank', 'population']
                available_cols = [col for col in needed_cols if col in gdf.columns]
                
                # Create a clean dataframe without problematic columns
                clean_df = gdf[available_cols].copy()
                top_10 = clean_df.nsmallest(10, 'composite_rank')
                
                top_wards_html = "<h3>🎯 Top 10 Highest Risk Wards</h3><table class='data-table'><tr><th>Rank</th><th>Ward</th><th>LGA</th><th>Risk Score</th><th>Population</th></tr>"
                for _, row in top_10.iterrows():
                    top_wards_html += f"<tr><td>{int(row['composite_rank'])}</td><td>{row['ward_name']}</td><td>{row.get('lga_name', 'N/A')}</td><td>{row['composite_score']:.3f}</td><td>{int(row.get('population', 0)):,}</td></tr>"
                top_wards_html += "</table>"
        except Exception as e:
            logger.warning(f"Error creating top wards table: {e}")
            top_wards_html = "<p>Top wards table could not be generated.</p>"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>ITN Distribution Analysis Dashboard - {export_data.get('session_id', 'Results')}</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ box-sizing: border-box; margin: 0; padding: 0; }}
                body {{ 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
                    color: #333;
                    line-height: 1.6;
                    padding: 20px;
                }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                
                /* Header */
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 40px;
                    border-radius: 20px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                    text-align: center;
                    margin-bottom: 30px;
                }}
                .header h1 {{
                    font-size: 2.5em;
                    margin-bottom: 10px;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
                }}
                .header p {{ font-size: 1.2em; opacity: 0.9; }}
                
                /* Grid Layout */
                .grid {{ 
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                
                /* Cards */
                .card {{
                    background: white;
                    padding: 25px;
                    border-radius: 15px;
                    box-shadow: 0 5px 20px rgba(0,0,0,0.1);
                    transition: transform 0.3s ease;
                }}
                .card:hover {{ transform: translateY(-5px); }}
                
                .card h3 {{
                    color: #667eea;
                    margin-bottom: 15px;
                    font-size: 1.3em;
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }}
                
                /* Impact Cards */
                .impact-card {{
                    background: linear-gradient(45deg, #11998e, #38ef7d);
                    color: white;
                    text-align: center;
                }}
                .impact-number {{
                    font-size: 3em;
                    font-weight: bold;
                    margin: 10px 0;
                }}
                .impact-label {{ font-size: 1.1em; opacity: 0.9; }}
                
                /* Risk Cards */
                .risk-card {{ background: linear-gradient(45deg, #ee0979, #ff6a00); color: white; }}
                .coverage-card {{ background: linear-gradient(45deg, #4facfe, #00f2fe); color: white; }}
                
                /* Data Tables */
                .data-table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 15px;
                }}
                .data-table th {{
                    background: #667eea;
                    color: white;
                    padding: 10px;
                    text-align: left;
                }}
                .data-table td {{
                    padding: 10px;
                    border-bottom: 1px solid #eee;
                }}
                .data-table tr:hover {{ background: #f5f5f5; }}
                
                /* Progress Bar */
                .progress-bar {{
                    background: #e0e0e0;
                    height: 30px;
                    border-radius: 15px;
                    overflow: hidden;
                    margin: 15px 0;
                }}
                .progress-fill {{
                    background: linear-gradient(90deg, #667eea, #764ba2);
                    height: 100%;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: white;
                    font-weight: bold;
                }}
                
                /* Insights Section */
                .insights {{
                    background: white;
                    padding: 30px;
                    border-radius: 15px;
                    box-shadow: 0 5px 20px rgba(0,0,0,0.1);
                    margin-bottom: 30px;
                }}
                .insight-item {{
                    display: flex;
                    align-items: start;
                    margin: 15px 0;
                    padding: 15px;
                    background: #f8f9fa;
                    border-radius: 10px;
                    border-left: 4px solid #667eea;
                }}
                .insight-icon {{ font-size: 1.5em; margin-right: 15px; }}
                
                /* Footer */
                .footer {{
                    text-align: center;
                    padding: 20px;
                    color: #666;
                    font-size: 0.9em;
                }}
                
                /* Responsive */
                @media (max-width: 768px) {{
                    .header h1 {{ font-size: 2em; }}
                    .grid {{ grid-template-columns: 1fr; }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🦟 ITN Distribution Analysis Dashboard</h1>
                    <p>Comprehensive Malaria Risk Assessment & Net Distribution Plan</p>
                    <p>Generated: {export_data['export_date'].strftime('%B %d, %Y at %I:%M %p')}</p>
                </div>
                
                <div class="grid">
                    <div class="card impact-card">
                        <h3>👥 Population Impact</h3>
                        <div class="impact-number">{stats.get('covered_population', 0):,}</div>
                        <div class="impact-label">People Protected</div>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: {stats.get('coverage_percent', 0)}%">
                                {stats.get('coverage_percent', 0)}% Coverage
                            </div>
                        </div>
                    </div>
                    
                    <div class="card risk-card">
                        <h3>⚠️ High Risk Wards</h3>
                        <div class="impact-number">{high_risk_count + very_high_risk_count}</div>
                        <div class="impact-label">Wards Need Urgent Intervention</div>
                        <p style="margin-top: 10px; font-size: 0.9em;">
                            Very High Risk: {very_high_risk_count}<br>
                            High Risk: {high_risk_count}
                        </p>
                    </div>
                    
                    <div class="card coverage-card">
                        <h3>🛏️ Net Distribution</h3>
                        <div class="impact-number">{stats.get('allocated', 0):,}</div>
                        <div class="impact-label">Nets Allocated</div>
                        <p style="margin-top: 10px; font-size: 0.9em;">
                            {stats.get('fully_covered_wards', 0)} wards with 100% coverage<br>
                            {stats.get('partially_covered_wards', 0)} wards partially covered
                        </p>
                    </div>
                </div>
                
                <div class="insights">
                    <h2>📊 Key Insights & Recommendations</h2>
                    
                    <div class="insight-item">
                        <span class="insight-icon">🎯</span>
                        <div>
                            <strong>Strategic Focus:</strong> The distribution plan prioritizes {stats.get('prioritized_wards', 0)} wards 
                            with the highest malaria risk scores. These wards represent areas with critical combinations of 
                            high malaria prevalence, poor housing conditions, and vulnerable populations.
                        </div>
                    </div>
                    
                    <div class="insight-item">
                        <span class="insight-icon">📈</span>
                        <div>
                            <strong>Coverage Analysis:</strong> With {stats.get('total_nets', 0):,} nets available, 
                            we achieve {stats.get('coverage_percent', 0)}% population coverage. This focused approach ensures 
                            maximum impact in the highest-risk areas, though {100 - stats.get('coverage_percent', 0):.1f}% 
                            of the population remains uncovered.
                        </div>
                    </div>
                    
                    <div class="insight-item">
                        <span class="insight-icon">💡</span>
                        <div>
                            <strong>Next Steps:</strong> Consider supplementary interventions for uncovered areas including 
                            indoor residual spraying (IRS), community education programs, and environmental management to 
                            reduce mosquito breeding sites.
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    {top_wards_html}
                </div>
                
                <div class="card">
                    <h3>📈 Distribution Statistics</h3>
                    <div class="grid" style="grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));">
                        <div>
                            <strong>Total Wards:</strong> {export_data['total_wards']}<br>
                            <strong>Wards Receiving Nets:</strong> {stats.get('prioritized_wards', 0)}<br>
                            <strong>Analysis Method:</strong> {export_data['analysis_method'].title()}
                        </div>
                        <div>
                            <strong>Avg Ward Coverage:</strong> {stats.get('ward_coverage_stats', {{}}).get('avg_coverage_percent', 0):.1f}%<br>
                            <strong>Min Coverage:</strong> {stats.get('ward_coverage_stats', {{}}).get('min_coverage_percent', 0):.1f}%<br>
                            <strong>Max Coverage:</strong> {stats.get('ward_coverage_stats', {{}}).get('max_coverage_percent', 0):.1f}%
                        </div>
                        <div>
                            <strong>Total Population:</strong> {stats.get('total_population', 0):,}<br>
                            <strong>Protected Population:</strong> {stats.get('covered_population', 0):,}<br>
                            <strong>Remaining Nets:</strong> {stats.get('remaining', 0):,}
                        </div>
                    </div>
                </div>
                
                <div class="footer">
                    <p>This report was generated using advanced malaria risk analysis algorithms combining 
                    composite scoring and PCA methodologies. For detailed ward-level data, please refer to 
                    the accompanying CSV file.</p>
                    <p><strong>ChatMRPT</strong> - Malaria Risk Prioritization Tool | {export_data['export_date'].strftime('%Y')}</p>
                </div>
            </div>
        </body>
        </html>
        """
        return html
    
    def _copy_maps(self, export_data: Dict[str, Any], export_dir: Path, session_id: str) -> List[Path]:
        """Copy relevant map files to export directory"""
        copied_files = []
        
        try:
            # Copy vulnerability maps
            for map_file in export_data.get('vulnerability_maps', []):
                if map_file.exists():
                    dest = export_dir / f"vulnerability_{map_file.name}"
                    import shutil
                    shutil.copy2(map_file, dest)
                    copied_files.append(dest)
            
            # Copy ITN maps
            for map_file in export_data.get('itn_maps', []):
                if map_file.exists():
                    dest = export_dir / f"itn_{map_file.name}"
                    import shutil
                    shutil.copy2(map_file, dest)
                    copied_files.append(dest)
            
        except Exception as e:
            logger.error(f"Error copying maps: {e}")
        
        return copied_files
    
    def _create_summary_report(self, export_data: Dict[str, Any], export_dir: Path) -> Optional[Path]:
        """Create human-readable summary report"""
        try:
            stats = export_data.get('summary_stats', {})
            
            summary_text = f"""ITN DISTRIBUTION ANALYSIS SUMMARY
============================================
Generated: {export_data['export_date'].strftime('%B %d, %Y at %I:%M %p')}
Session ID: {export_data['session_id']}
Analysis Method: {export_data['analysis_method'].title()}

COVERAGE STATISTICS
-------------------
Total Wards Analyzed: {export_data['total_wards']}
Wards Receiving Nets: {stats.get('prioritized_wards', 'N/A')}
- Fully Covered (100%): {stats.get('fully_covered_wards', 'N/A')}
- Partially Covered: {stats.get('partially_covered_wards', 'N/A')}

NET DISTRIBUTION
----------------
Total Nets Available: {stats.get('total_nets', 'N/A'):,}
Nets Allocated: {stats.get('allocated', 'N/A'):,}
Nets Remaining: {stats.get('remaining', 'N/A'):,}

POPULATION IMPACT
-----------------
Total Population: {stats.get('total_population', 'N/A'):,}
Population Covered: {stats.get('covered_population', 'N/A'):,}
Coverage Percentage: {stats.get('coverage_percent', 'N/A')}%

WARD COVERAGE DETAILS
--------------------
Average Coverage: {stats.get('ward_coverage_stats', {}).get('avg_coverage_percent', 'N/A')}%
Minimum Coverage: {stats.get('ward_coverage_stats', {}).get('min_coverage_percent', 'N/A')}%
Maximum Coverage: {stats.get('ward_coverage_stats', {}).get('max_coverage_percent', 'N/A')}%

FILES INCLUDED IN THIS PACKAGE
------------------------------
1. itn_distribution_results.csv - Detailed ward-level data
2. itn_distribution_dashboard.html - Interactive visualization
3. Vulnerability and ITN distribution maps
4. This summary report

NOTES
-----
- Wards are prioritized based on composite malaria risk scores
- Each household receives 2 nets (WHO standard)
- Full coverage means 100% of households in a ward receive nets
"""
            
            summary_path = export_dir / 'Analysis_Summary.txt'
            with open(summary_path, 'w') as f:
                f.write(summary_text)
            
            return summary_path
            
        except Exception as e:
            logger.error(f"Error creating summary report: {e}")
            return None
    
    def _create_metadata(self, export_data: Dict[str, Any], export_dir: Path) -> Optional[Path]:
        """Create metadata file with export information"""
        try:
            metadata = {
                'export_type': 'itn_distribution',
                'export_date': export_data['export_date'].isoformat(),
                'session_id': export_data['session_id'],
                'analysis_method': export_data['analysis_method'],
                'total_wards': export_data['total_wards'],
                'files_included': {
                    'dashboard': 'itn_distribution_dashboard.html',
                    'csv': 'itn_distribution_results.csv',
                    'maps': [f.name for f in export_dir.glob('*.html')]
                },
                'summary_stats': export_data.get('summary_stats', {})
            }
            
            metadata_path = export_dir / 'export_metadata.json'
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            return metadata_path
            
        except Exception as e:
            logger.error(f"Error creating metadata: {e}")
            return None
    
    def _create_zip_package(self, files: List[Path], export_dir: Path, session_id: str) -> Path:
        """Create ZIP file with all export components"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_name = f"ITN_Distribution_Export_{timestamp}.zip"
        zip_path = export_dir.parent / zip_name
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files:
                if file_path.exists():
                    arcname = file_path.relative_to(export_dir)
                    zipf.write(file_path, arcname)
        
        return zip_path
    
    def _format_export_summary(self, export_data: Dict[str, Any], file_count: int, size_mb: float) -> str:
        """Format export summary message"""
        stats = export_data.get('summary_stats', {})
        
        message = f"""**ITN Distribution Export Package Created Successfully!**

**Package Contents:**
- Interactive Dashboard (HTML)
- Detailed Results (CSV) with {export_data['total_wards']} wards
- Visualization Maps

**Key Statistics:**
- Total Nets: {stats.get('total_nets', 'N/A'):,}
- Population Coverage: {stats.get('coverage_percent', 'N/A')}%
- Prioritized Wards: {stats.get('prioritized_wards', 'N/A')}

**Package Details:**
- Files Included: {file_count}
- Package Size: {size_mb:.1f} MB
- Export Date: {export_data['export_date'].strftime('%Y-%m-%d %H:%M')}"""
        
        return message