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

from app.utils.tool_base import BaseTool, ToolCategory, ToolExecutionResult, get_session_unified_dataset
from app.services.dataset_builder import load_unified_dataset

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

    year_tag: str = Field(
        '',
        description="Year suffix for multi-year datasets e.g. '_2022'. Empty string uses aggregate."
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

                    # Also save a copy in the main uploads folder for SQL queries
                    uploads_csv_path = Path(f"instance/uploads/{session_id}/itn_distribution_results.csv")
                    import shutil
                    shutil.copy2(csv_path, uploads_csv_path)
                    logger.info(f"Saved copy to uploads folder: {uploads_csv_path}")
            
            # 2. Generate dashboard if requested
            if self.include_dashboard:
                logger.info(f"include_dashboard is True, generating dashboard...")
                dashboard_path = self._generate_dashboard(export_data, export_dir, session_id)
                if dashboard_path:
                    exported_files.append(dashboard_path)
                    logger.info(f"Dashboard added to export list: {dashboard_path}")
                else:
                    logger.error("Dashboard generation returned None")
            else:
                logger.info("include_dashboard is False, skipping dashboard generation")
            
            # 3. Copy maps if requested
            if self.include_maps:
                map_files = self._copy_maps(export_data, export_dir, session_id)
                exported_files.extend(map_files)
                logger.info(f"Copied {len(map_files)} map files")
            
            # 4. Create summary report instead of JSON metadata
            summary_path = self._create_summary_report(export_data, export_dir)
            if summary_path:
                exported_files.append(summary_path)
            
            # Log all files before creating ZIP
            logger.info(f"Total files to be zipped: {len(exported_files)}")
            for i, file in enumerate(exported_files):
                logger.info(f"  File {i+1}: {file.name} (exists: {file.exists()})")
            
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
            gdf = load_unified_dataset(session_id, require_geometry=True, year_tag=self.year_tag)
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
            logger.info(f"Starting dashboard generation for session {session_id}")
            logger.info(f"Export directory: {export_dir}")
            
            # Create dashboard HTML
            dashboard_html = self._create_dashboard_html(export_data)
            logger.info(f"Dashboard HTML created, length: {len(dashboard_html)} characters")
            
            dashboard_path = export_dir / 'itn_distribution_dashboard.html'
            logger.info(f"Writing dashboard to: {dashboard_path}")
            
            with open(dashboard_path, 'w', encoding='utf-8') as f:
                f.write(dashboard_html)
            
            # Verify the file was created
            if dashboard_path.exists():
                logger.info(f"Dashboard successfully created at {dashboard_path}, size: {dashboard_path.stat().st_size} bytes")
            else:
                logger.error(f"Dashboard file not found after writing: {dashboard_path}")
                return None
            
            return dashboard_path
            
        except Exception as e:
            logger.error(f"Error generating dashboard: {e}", exc_info=True)
            return None
    
    def _create_dashboard_html(self, export_data: Dict[str, Any]) -> str:
        """Create modern, interactive dashboard HTML with 2025 design trends"""
        stats = export_data.get('summary_stats', {})
        gdf = export_data['unified_dataset']
        itn_results = export_data.get('itn_results', {})
        session_id = export_data.get('session_id', '')
        
        # Extract data for visualizations
        ward_col = next((col for col in gdf.columns if col.lower() in ['wardname', 'ward_name', 'ward']), 'WardName')
        lga_col = next((col for col in gdf.columns if col.upper() == 'LGA' or col.lower() in ['lga_name', 'lganame']), 'LGA')
        
        # Get risk categories data
        category_col = None
        if 'composite_category' in gdf.columns:
            category_col = 'composite_category'
        elif 'vulnerability_category' in gdf.columns:
            category_col = 'vulnerability_category'
        
        # Calculate risk distribution
        risk_distribution = {"Very High Risk": 0, "High Risk": 0, "Medium Risk": 0, "Low Risk": 0}
        if category_col and category_col in gdf.columns:
            # Handle categorical columns properly
            if hasattr(gdf[category_col], 'cat'):
                # Convert categorical to string first to avoid issues with new categories
                categories = gdf[category_col].astype(str).fillna('Unknown')
            else:
                categories = gdf[category_col].fillna('Unknown')
            
            for category in categories:
                if isinstance(category, str):
                    if 'very high' in category.lower():
                        risk_distribution["Very High Risk"] += 1
                    elif 'high' in category.lower():
                        risk_distribution["High Risk"] += 1
                    elif 'medium' in category.lower():
                        risk_distribution["Medium Risk"] += 1
                    elif 'low' in category.lower():
                        risk_distribution["Low Risk"] += 1
        
        # Get top 10 wards data for chart
        top_wards_data = []
        if 'composite_rank' in gdf.columns and ward_col in gdf.columns:
            top_10 = gdf.nsmallest(10, 'composite_rank')
            for _, row in top_10.iterrows():
                top_wards_data.append({
                    'ward': str(row.get(ward_col, 'Unknown')),
                    'score': float(row.get('composite_score', 0)),
                    'rank': int(row.get('composite_rank', 0))
                })
        
        # Get state name - check multiple possible column names and LGA patterns
        state_name = "Analysis Region"
        
        # First check explicit state columns
        state_columns = ['state', 'State', 'STATE', 'state_name', 'StateName', 'State_Name']
        for col in state_columns:
            if col in gdf.columns and not gdf[col].empty:
                # Get the most common state name (in case of variations)
                state_value = gdf[col].mode()[0] if len(gdf[col].mode()) > 0 else gdf[col].iloc[0]
                if pd.notna(state_value) and str(state_value).strip():
                    state_name = str(state_value).strip()
                    break
        
        # If no state column, try to infer from LGA names (e.g., "Adamawa" in LGA names)
        if state_name == "Analysis Region" and lga_col and lga_col in gdf.columns:
            # Check if LGAs contain common state patterns
            lga_values = gdf[lga_col].dropna().astype(str)
            if not lga_values.empty:
                # Common Nigerian states that might appear in data
                common_states = ['Adamawa', 'Kano', 'Lagos', 'Kaduna', 'Katsina', 'Oyo', 'Rivers', 'Bauchi', 'Jigawa', 'Benue']
                for state in common_states:
                    if any(state.lower() in lga.lower() for lga in lga_values):
                        state_name = f"{state} State"
                        break
        
        # Format state name properly
        if state_name != "Analysis Region" and not state_name.endswith("State"):
            state_name = f"{state_name} State"
        
        # Generate ward data for the table
        ward_table_data = []
        if ward_col in gdf.columns and lga_col in gdf.columns:
            # Sort by composite rank if available
            if 'composite_rank' in gdf.columns:
                sorted_gdf = gdf.sort_values('composite_rank')
            else:
                sorted_gdf = gdf
            
            for _, row in sorted_gdf.iterrows():
                ward_name = str(row.get(ward_col, 'Unknown'))
                lga_name = str(row.get(lga_col, 'Unknown'))
                risk_score = f"{float(row.get('composite_score', 0)):.3f}" if 'composite_score' in row else 'N/A'
                risk_category = str(row.get('composite_category', row.get('vulnerability_category', 'Unknown')))
                population = f"{int(row.get('population', 0)):,}" if 'population' in row and pd.notna(row.get('population')) else 'N/A'
                
                # Calculate ITN coverage if available
                coverage = 'N/A'
                if 'nets_allocated' in row and 'nets_needed' in row:
                    if pd.notna(row['nets_allocated']) and pd.notna(row['nets_needed']) and row['nets_needed'] > 0:
                        coverage_pct = (row['nets_allocated'] / row['nets_needed']) * 100
                        coverage = f"{coverage_pct:.1f}%"
                elif 'coverage_percent' in row and pd.notna(row['coverage_percent']):
                    coverage = f"{float(row['coverage_percent']):.1f}%"
                
                ward_table_data.append([
                    ward_name,
                    lga_name,
                    risk_score,
                    risk_category,
                    population,
                    coverage
                ])
        
        # Convert to JSON for JavaScript
        import json
        ward_table_json = json.dumps(ward_table_data[:100])  # Limit to first 100 for performance
        
        # Generate specific insights based on data
        total_high_risk = risk_distribution.get('Very High Risk', 0) + risk_distribution.get('High Risk', 0)
        uncovered_percent = 100 - stats.get('coverage_percent', 0)
        uncovered_population = stats.get('total_population', 0) - stats.get('population_covered', 0)
        
        # Calculate top affected LGAs
        if lga_col in gdf.columns and 'composite_score' in gdf.columns:
            lga_risk_scores = gdf.groupby(lga_col)['composite_score'].mean().sort_values(ascending=False)
            top_lgas = list(lga_risk_scores.head(3).index)
        else:
            top_lgas = []
        
        # Generate specific recommendations based on coverage
        recommendations = []
        if uncovered_percent > 40:
            recommendations.append(f"Secure additional {int(uncovered_population / 900):,} nets to achieve full coverage for remaining {uncovered_percent:.1f}% population")
        else:
            recommendations.append(f"Focus on the {stats.get('partially_covered_wards', 0)} partially covered wards to maximize impact")
        
        if total_high_risk > 50:
            recommendations.append(f"Implement emergency intervention in {total_high_risk} high-risk wards within next 30 days")
        
        if top_lgas:
            recommendations.append(f"Prioritize resources for {', '.join(top_lgas[:2])} LGAs showing highest average risk scores")
        
        # Generate modern dashboard HTML
        html = f"""
<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Malaria Risk Analysis Dashboard - {state_name}</title>
    
    <!-- Inline styles to avoid Tailwind CDN in production -->
    
    <!-- Plotly.js -->
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    
    <!-- DataTables -->
    <link rel="stylesheet" href="https://cdn.datatables.net/2.0.0/css/dataTables.tailwindcss.css">
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.datatables.net/2.0.0/js/dataTables.js"></script>
    <script src="https://cdn.datatables.net/2.0.0/js/dataTables.tailwindcss.js"></script>
    
    <!-- Modern CSS Framework without CDN dependency -->
    
    <style>
        /* Modern CSS Framework - No external dependencies */
        :root {{
            --primary: #6366F1;
            --secondary: #EC4899;
            --accent: #10B981;
            --dark: #1F2937;
            --light: #F9FAFB;
            --white: #FFFFFF;
            --gray-50: #F9FAFB;
            --gray-100: #F3F4F6;
            --gray-200: #E5E7EB;
            --gray-300: #D1D5DB;
            --gray-400: #9CA3AF;
            --gray-500: #6B7280;
            --gray-600: #4B5563;
            --gray-700: #374151;
            --gray-800: #1F2937;
            --gray-900: #111827;
            --gradient-primary: linear-gradient(135deg, #6366F1 0%, #8B5CF6 100%);
            --gradient-secondary: linear-gradient(135deg, #EC4899 0%, #F472B6 100%);
            --gradient-success: linear-gradient(135deg, #10B981 0%, #34D399 100%);
            --gradient-danger: linear-gradient(135deg, #EF4444 0%, #F87171 100%);
            --gradient-info: linear-gradient(135deg, #3B82F6 0%, #60A5FA 100%);
        }}
        
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--light);
            color: var(--gray-800);
            line-height: 1.6;
        }}
        
        /* Layout Classes */
        .container {{
            max-width: 1280px;
            margin: 0 auto;
            padding: 0 1rem;
        }}
        
        .grid {{
            display: grid;
            gap: 1.5rem;
        }}
        
        .grid-cols-1 {{ grid-template-columns: repeat(1, 1fr); }}
        .grid-cols-2 {{ grid-template-columns: repeat(2, 1fr); }}
        .grid-cols-3 {{ grid-template-columns: repeat(3, 1fr); }}
        .grid-cols-4 {{ grid-template-columns: repeat(4, 1fr); }}
        
        @media (min-width: 768px) {{
            .md\:grid-cols-2 {{ grid-template-columns: repeat(2, 1fr); }}
            .md\:grid-cols-3 {{ grid-template-columns: repeat(3, 1fr); }}
            .md\:grid-cols-4 {{ grid-template-columns: repeat(4, 1fr); }}
        }}
        
        @media (min-width: 1024px) {{
            .lg\:grid-cols-2 {{ grid-template-columns: repeat(2, 1fr); }}
        }}
        
        /* Flexbox Classes */
        .flex {{ display: flex; }}
        .flex-col {{ flex-direction: column; }}
        .items-center {{ align-items: center; }}
        .items-start {{ align-items: flex-start; }}
        .justify-between {{ justify-content: space-between; }}
        .justify-center {{ justify-content: center; }}
        .gap-2 {{ gap: 0.5rem; }}
        .gap-4 {{ gap: 1rem; }}
        .gap-6 {{ gap: 1.5rem; }}
        .gap-8 {{ gap: 2rem; }}
        
        /* Spacing Classes */
        .p-4 {{ padding: 1rem; }}
        .p-6 {{ padding: 1.5rem; }}
        .p-8 {{ padding: 2rem; }}
        .px-4 {{ padding-left: 1rem; padding-right: 1rem; }}
        .px-6 {{ padding-left: 1.5rem; padding-right: 1.5rem; }}
        .px-8 {{ padding-left: 2rem; padding-right: 2rem; }}
        .py-2 {{ padding-top: 0.5rem; padding-bottom: 0.5rem; }}
        .py-3 {{ padding-top: 0.75rem; padding-bottom: 0.75rem; }}
        .py-4 {{ padding-top: 1rem; padding-bottom: 1rem; }}
        .py-10 {{ padding-top: 2.5rem; padding-bottom: 2.5rem; }}
        .py-16 {{ padding-top: 4rem; padding-bottom: 4rem; }}
        .mt-2 {{ margin-top: 0.5rem; }}
        .mt-4 {{ margin-top: 1rem; }}
        .mt-6 {{ margin-top: 1.5rem; }}
        .mt-10 {{ margin-top: 2.5rem; }}
        .mb-2 {{ margin-bottom: 0.5rem; }}
        .mb-4 {{ margin-bottom: 1rem; }}
        .mb-6 {{ margin-bottom: 1.5rem; }}
        .mb-8 {{ margin-bottom: 2rem; }}
        .mb-12 {{ margin-bottom: 3rem; }}
        .mr-2 {{ margin-right: 0.5rem; }}
        .mr-3 {{ margin-right: 0.75rem; }}
        .mr-4 {{ margin-right: 1rem; }}
        .ml-auto {{ margin-left: auto; }}
        .space-y-1 > * + * {{ margin-top: 0.25rem; }}
        .space-y-3 > * + * {{ margin-top: 0.75rem; }}
        
        /* Typography Classes */
        .text-xs {{ font-size: 0.75rem; }}
        .text-sm {{ font-size: 0.875rem; }}
        .text-base {{ font-size: 1rem; }}
        .text-lg {{ font-size: 1.125rem; }}
        .text-xl {{ font-size: 1.25rem; }}
        .text-2xl {{ font-size: 1.5rem; }}
        .text-3xl {{ font-size: 1.875rem; }}
        .text-4xl {{ font-size: 2.25rem; }}
        .text-5xl {{ font-size: 3rem; }}
        .text-6xl {{ font-size: 3.75rem; }}
        .font-medium {{ font-weight: 500; }}
        .font-semibold {{ font-weight: 600; }}
        .font-bold {{ font-weight: 700; }}
        .uppercase {{ text-transform: uppercase; }}
        .tracking-wide {{ letter-spacing: 0.025em; }}
        .tracking-wider {{ letter-spacing: 0.05em; }}
        .text-center {{ text-align: center; }}
        .text-left {{ text-align: left; }}
        .text-white {{ color: white; }}
        .text-gray-500 {{ color: var(--gray-500); }}
        .text-gray-600 {{ color: var(--gray-600); }}
        .text-gray-700 {{ color: var(--gray-700); }}
        .text-gray-800 {{ color: var(--gray-800); }}
        .text-gray-900 {{ color: var(--gray-900); }}
        .text-indigo-500 {{ color: #6366F1; }}
        .text-indigo-900 {{ color: #312E81; }}
        .text-emerald-500 {{ color: #10B981; }}
        .text-emerald-900 {{ color: #064E3B; }}
        .text-red-900 {{ color: #7F1D1D; }}
        .text-yellow-900 {{ color: #78350F; }}
        .text-green-900 {{ color: #14532D; }}
        
        /* Background Classes */
        .bg-white {{ background-color: white; }}
        .bg-light {{ background-color: var(--light); }}
        .bg-gray-50 {{ background-color: var(--gray-50); }}
        .bg-gray-100 {{ background-color: var(--gray-100); }}
        .bg-gray-200 {{ background-color: var(--gray-200); }}
        .bg-primary {{ background-color: var(--primary); }}
        .bg-green-500 {{ background-color: #10B981; }}
        .bg-blue-500 {{ background-color: #3B82F6; }}
        .bg-indigo-50 {{ background-color: #EEF2FF; }}
        .bg-purple-50 {{ background-color: #FAF5FF; }}
        .bg-emerald-50 {{ background-color: #ECFDF5; }}
        .bg-teal-50 {{ background-color: #F0FDFA; }}
        .bg-red-50 {{ background-color: #FEF2F2; }}
        .bg-yellow-50 {{ background-color: #FFFBEB; }}
        .bg-green-50 {{ background-color: #F0FDF4; }}
        .bg-gradient-primary {{ background: var(--gradient-primary); }}
        .bg-gradient-to-br {{ background: linear-gradient(to bottom right, var(--from-color), var(--to-color)); }}
        .from-indigo-50 {{ --from-color: #EEF2FF; }}
        .to-purple-50 {{ --to-color: #FAF5FF; }}
        .from-emerald-50 {{ --from-color: #ECFDF5; }}
        .to-teal-50 {{ --to-color: #F0FDFA; }}
        .from-emerald-500 {{ --from-color: #10B981; }}
        .to-teal-600 {{ --to-color: #0891B2; }}
        .from-blue-500 {{ --from-color: #3B82F6; }}
        .to-indigo-600 {{ --to-color: #4F46E5; }}
        .from-purple-500 {{ --from-color: #A855F7; }}
        .to-pink-600 {{ --to-color: #DB2777; }}
        
        /* Border Classes */
        .border {{ border: 1px solid var(--gray-300); }}
        .border-0 {{ border: 0; }}
        .border-l-4 {{ border-left-width: 4px; }}
        .border-gray-200 {{ border-color: var(--gray-200); }}
        .border-indigo-200 {{ border-color: #C7D2FE; }}
        .border-emerald-200 {{ border-color: #A7F3D0; }}
        .border-red-500 {{ border-color: #EF4444; }}
        .border-yellow-500 {{ border-color: #F59E0B; }}
        .border-green-500 {{ border-color: #10B981; }}
        .divide-y > * + * {{ border-top: 1px solid var(--gray-200); }}
        .divide-gray-200 > * + * {{ border-color: var(--gray-200); }}
        
        /* Border Radius Classes */
        .rounded {{ border-radius: 0.25rem; }}
        .rounded-lg {{ border-radius: 0.5rem; }}
        .rounded-xl {{ border-radius: 0.75rem; }}
        .rounded-2xl {{ border-radius: 1rem; }}
        .rounded-full {{ border-radius: 9999px; }}
        
        /* Shadow Classes */
        .shadow {{ box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06); }}
        .shadow-lg {{ box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05); }}
        
        /* Opacity Classes */
        .opacity-80 {{ opacity: 0.8; }}
        .opacity-90 {{ opacity: 0.9; }}
        
        /* Position Classes */
        .relative {{ position: relative; }}
        .absolute {{ position: absolute; }}
        .inset-0 {{ top: 0; right: 0; bottom: 0; left: 0; }}
        .z-10 {{ z-index: 10; }}
        
        /* Display Classes */
        .block {{ display: block; }}
        .inline-block {{ display: inline-block; }}
        .hidden {{ display: none; }}
        
        /* Overflow Classes */
        .overflow-hidden {{ overflow: hidden; }}
        .overflow-x-auto {{ overflow-x: auto; }}
        
        /* Width/Height Classes */
        .w-2 {{ width: 0.5rem; }}
        .w-full {{ width: 100%; }}
        .h-3 {{ height: 0.75rem; }}
        .h-8 {{ height: 2rem; }}
        .h-full {{ height: 100%; }}
        .min-w-full {{ min-width: 100%; }}
        .max-w-7xl {{ max-width: 80rem; }}
        .mx-auto {{ margin-left: auto; margin-right: auto; }}
        
        /* Transition Classes */
        .transition-all {{ transition-property: all; transition-timing-function: cubic-bezier(0.4, 0, 0.2, 1); transition-duration: 150ms; }}
        .transition-colors {{ transition-property: background-color, border-color, color, fill, stroke; transition-timing-function: cubic-bezier(0.4, 0, 0.2, 1); transition-duration: 150ms; }}
        .duration-1000 {{ transition-duration: 1000ms; }}
        .ease-out {{ transition-timing-function: cubic-bezier(0, 0, 0.2, 1); }}
        
        /* Hover Classes */
        .hover\:bg-gray-200:hover {{ background-color: var(--gray-200); }}
        .hover\:bg-gray-700:hover {{ background-color: var(--gray-700); }}
        .hover\:bg-indigo-700:hover {{ background-color: #4338CA; }}
        .hover\:bg-green-600:hover {{ background-color: #059669; }}
        .hover\:bg-blue-600:hover {{ background-color: #2563EB; }}
        
        /* Animation Classes */
        @keyframes fadeIn {{
            from {{ opacity: 0; }}
            to {{ opacity: 1; }}
        }}
        
        @keyframes slideUp {{
            from {{ transform: translateY(10px); opacity: 0; }}
            to {{ transform: translateY(0); opacity: 1; }}
        }}
        
        .animate-fade-in {{ animation: fadeIn 0.5s ease-in-out; }}
        .animate-slide-up {{ animation: slideUp 0.3s ease-out; }}
        
        /* Button Classes */
        .btn {{
            display: inline-block;
            padding: 0.5rem 1rem;
            border-radius: 0.5rem;
            font-weight: 500;
            transition: all 0.15s;
            cursor: pointer;
            border: none;
            text-decoration: none;
        }}
        
        .btn:hover {{
            transform: translateY(-1px);
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        }}
        
        /* Table Classes */
        table {{
            border-collapse: collapse;
            width: 100%;
        }}
        
        th {{
            text-align: left;
            font-weight: 500;
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--gray-500);
            padding: 0.75rem 1.5rem;
            background-color: var(--gray-50);
        }}
        
        td {{
            padding: 1rem 1.5rem;
            border-top: 1px solid var(--gray-200);
        }}
        
        tbody tr:hover {{
            background-color: var(--gray-50);
        }}
        
        /* Animated background */
        .hero-bg {{
            background: var(--gradient-primary);
            position: relative;
            overflow: hidden;
        }}
        
        .hero-bg::before {{
            content: '';
            position: absolute;
            top: -50%;
            right: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
            animation: rotate 30s linear infinite;
        }}
        
        @keyframes rotate {{
            from {{ transform: rotate(0deg); }}
            to {{ transform: rotate(360deg); }}
        }}
        
        /* Glassmorphism effects */
        .glass {{
            background: rgba(255, 255, 255, 0.9);
            backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.3);
        }}
        
        /* Modern card hover effects */
        .card-hover {{
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }}
        
        .card-hover:hover {{
            transform: translateY(-4px);
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
        }}
        
        /* Animated counters */
        .counter {{
            animation: slideUp 0.6s ease-out;
        }}
        
        /* Custom scrollbar */
        ::-webkit-scrollbar {{
            width: 10px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: #F3F4F6;
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: #9CA3AF;
            border-radius: 5px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: #6B7280;
        }}
        
        /* Tab styles */
        .tab-active {{
            background: var(--gradient-primary);
            color: white;
        }}
        
        /* Loading animation */
        .loading {{
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 3px solid rgba(255, 255, 255, 0.3);
            border-radius: 50%;
            border-top-color: white;
            animation: spin 1s ease-in-out infinite;
        }}
        
        @keyframes spin {{
            to {{ transform: rotate(360deg); }}
        }}
        
        /* Print styles */
        @media print {{
            body {{ background: white; }}
            .no-print {{ display: none !important; }}
            .page-break {{ page-break-after: always; }}
        }}
    </style>
</head>
<body class="bg-light">
    <!-- Hero Section -->
    <div class="hero-bg text-white py-16 px-8 relative">
        <div class="max-w-7xl mx-auto text-center relative z-10">
            <h1 class="text-5xl font-bold mb-4 animate-fade-in">
                Malaria Risk Analysis Dashboard
            </h1>
            <p class="text-xl opacity-90 mb-2">{state_name}</p>
            <p class="text-lg opacity-80">Generated on {export_data['export_date'].strftime('%B %d, %Y at %I:%M %p')}</p>
            
            <!-- Quick Stats -->
            <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mt-10">
                <div class="glass rounded-xl p-6 text-center animate-slide-up">
                    <div class="text-4xl font-bold mb-2 counter">{export_data['total_wards']:,}</div>
                    <div class="text-sm uppercase tracking-wide opacity-90">Total Wards</div>
                </div>
                <div class="glass rounded-xl p-6 text-center animate-slide-up" style="animation-delay: 0.1s">
                    <div class="text-4xl font-bold mb-2 counter">{stats.get('total_population', 0):,}</div>
                    <div class="text-sm uppercase tracking-wide opacity-90">Total Population</div>
                </div>
                <div class="glass rounded-xl p-6 text-center animate-slide-up" style="animation-delay: 0.2s">
                    <div class="text-4xl font-bold mb-2 counter">{stats.get('coverage_percent', 0)}%</div>
                    <div class="text-sm uppercase tracking-wide opacity-90">Coverage Rate</div>
                </div>
                <div class="glass rounded-xl p-6 text-center animate-slide-up" style="animation-delay: 0.3s">
                    <div class="text-4xl font-bold mb-2 counter">{stats.get('allocated', 0):,}</div>
                    <div class="text-sm uppercase tracking-wide opacity-90">Nets Allocated</div>
                </div>
            </div>
        </div>
    </div>
    
    <!-- Main Content -->
    <div class="max-w-7xl mx-auto px-4 py-10">
        <!-- Risk Analysis Overview -->
        <section class="mb-12 animate-fade-in">
            <h2 class="text-3xl font-bold text-dark mb-8 flex items-center">
                <span class="w-2 h-8 bg-gradient-to-b from-primary to-secondary rounded mr-4"></span>
                Risk Analysis Overview
            </h2>
            
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
                <!-- Risk Distribution Chart -->
                <div class="bg-white rounded-2xl shadow-lg p-6 card-hover">
                    <h3 class="text-xl font-semibold mb-4 text-gray-800">Risk Category Distribution</h3>
                    <div id="riskDistributionChart" style="height: 350px;"></div>
                </div>
                
                <!-- Top 10 Wards Chart -->
                <div class="bg-white rounded-2xl shadow-lg p-6 card-hover">
                    <h3 class="text-xl font-semibold mb-4 text-gray-800">Top 10 High-Risk Wards</h3>
                    <div id="topWardsChart" style="height: 350px;"></div>
                </div>
            </div>
        </section>
        
        <!-- ITN Distribution Dashboard -->
        <section class="mb-12 animate-fade-in">
            <h2 class="text-3xl font-bold text-dark mb-8 flex items-center">
                <span class="w-2 h-8 bg-gradient-to-b from-accent to-teal-400 rounded mr-4"></span>
                ITN Distribution Analysis
            </h2>
            
            <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
                <!-- Coverage Progress -->
                <div class="bg-gradient-to-br from-emerald-500 to-teal-600 rounded-2xl p-6 text-white card-hover">
                    <h3 class="text-lg font-semibold mb-4 opacity-90">Population Coverage</h3>
                    <div class="text-4xl font-bold mb-4">{stats.get('population_covered', 0):,}</div>
                    <div class="text-sm opacity-80 mb-4">People Protected</div>
                    <div class="w-full bg-white/20 rounded-full h-3 overflow-hidden">
                        <div class="h-full bg-white rounded-full transition-all duration-1000 ease-out"
                             style="width: {stats.get('coverage_percent', 0)}%"></div>
                    </div>
                    <div class="text-sm mt-2 opacity-90">{stats.get('coverage_percent', 0)}% of total population</div>
                </div>
                
                <!-- Ward Coverage -->
                <div class="bg-gradient-to-br from-blue-500 to-indigo-600 rounded-2xl p-6 text-white card-hover">
                    <h3 class="text-lg font-semibold mb-4 opacity-90">Ward Coverage</h3>
                    <div class="space-y-3">
                        <div class="flex justify-between items-center">
                            <span class="text-sm opacity-80">Fully Covered</span>
                            <span class="text-2xl font-bold">{stats.get('fully_covered_wards', 0)}</span>
                        </div>
                        <div class="flex justify-between items-center">
                            <span class="text-sm opacity-80">Partially Covered</span>
                            <span class="text-2xl font-bold">{stats.get('partially_covered_wards', 0)}</span>
                        </div>
                        <div class="flex justify-between items-center">
                            <span class="text-sm opacity-80">Not Covered</span>
                            <span class="text-2xl font-bold">{export_data['total_wards'] - stats.get('wards_with_nets', stats.get('prioritized_wards', 0))}</span>
                        </div>
                    </div>
                </div>
                
                <!-- Net Utilization -->
                <div class="bg-gradient-to-br from-purple-500 to-pink-600 rounded-2xl p-6 text-white card-hover">
                    <h3 class="text-lg font-semibold mb-4 opacity-90">Net Utilization</h3>
                    <div class="text-4xl font-bold mb-2">{(stats.get('allocated', 0) / stats.get('total_nets', 1) * 100):.1f}%</div>
                    <div class="text-sm opacity-80 mb-4">Nets Distributed</div>
                    <div class="space-y-2 text-sm">
                        <div class="flex justify-between">
                            <span class="opacity-80">Available:</span>
                            <span>{stats.get('total_nets', 0):,}</span>
                        </div>
                        <div class="flex justify-between">
                            <span class="opacity-80">Allocated:</span>
                            <span>{stats.get('allocated', 0):,}</span>
                        </div>
                        <div class="flex justify-between">
                            <span class="opacity-80">Remaining:</span>
                            <span>{stats.get('remaining', 0):,}</span>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Coverage Visualization -->
            <div class="bg-white rounded-2xl shadow-lg p-6 mt-6 card-hover">
                <h3 class="text-xl font-semibold mb-4 text-gray-800">Coverage Distribution by Ward</h3>
                <div id="coverageChart" style="height: 300px;"></div>
            </div>
        </section>
        
        <!-- Interactive Maps Gallery -->
        <section class="mb-12 animate-fade-in">
            <h2 class="text-3xl font-bold text-dark mb-8 flex items-center">
                <span class="w-2 h-8 bg-gradient-to-b from-secondary to-rose-400 rounded mr-4"></span>
                Interactive Maps
            </h2>
            
            <div class="bg-white rounded-2xl shadow-lg p-6">
                <!-- Map Tabs -->
                <div class="flex flex-wrap gap-2 mb-6 border-b border-gray-200">
                    <button class="tab px-6 py-3 rounded-t-lg font-medium transition-all tab-active" 
                            onclick="showMap('risk')">Risk Map</button>
                    <button class="tab px-6 py-3 rounded-t-lg font-medium transition-all hover:bg-gray-100" 
                            onclick="showMap('itn')">ITN Coverage</button>
                    <button class="tab px-6 py-3 rounded-t-lg font-medium transition-all hover:bg-gray-100" 
                            onclick="showMap('tpr')">TPR Distribution</button>
                </div>
                
                <!-- Map Container -->
                <div id="mapContainer" class="relative bg-gray-50 rounded-lg" style="height: 500px;">
                    <div class="absolute inset-0 flex items-center justify-center">
                        <div class="text-center">
                            <div class="text-6xl mb-4">🗺️</div>
                            <p class="text-gray-600">Interactive maps will be displayed here</p>
                            <p class="text-sm text-gray-500 mt-2">Maps are included as separate HTML files in the export package</p>
                        </div>
                    </div>
                </div>
                
                <!-- Map Controls -->
                <div class="flex justify-between items-center mt-4 no-print">
                    <div class="flex gap-2">
                        <button class="px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors">
                            <span class="text-xl">🔍</span> Zoom In
                        </button>
                        <button class="px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors">
                            <span class="text-xl">🔎</span> Zoom Out
                        </button>
                        <button class="px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors">
                            <span class="text-xl">📥</span> Download Map
                        </button>
                    </div>
                    <button class="px-4 py-2 bg-primary text-white rounded-lg hover:bg-indigo-700 transition-colors">
                        <span class="text-xl">⛶</span> Fullscreen
                    </button>
                </div>
            </div>
        </section>
        
        <!-- Ward-Level Data Explorer -->
        <section class="mb-12 animate-fade-in">
            <h2 class="text-3xl font-bold text-dark mb-8 flex items-center">
                <span class="w-2 h-8 bg-gradient-to-b from-yellow-400 to-orange-500 rounded mr-4"></span>
                Ward-Level Data Explorer
            </h2>
            
            <div class="bg-white rounded-2xl shadow-lg p-6">
                <div class="mb-4 flex justify-between items-center">
                    <p class="text-gray-600">Explore detailed data for all {export_data['total_wards']} wards</p>
                    <div class="flex gap-2 no-print">
                        <button class="px-4 py-2 bg-green-500 text-white rounded-lg hover:bg-green-600 transition-colors">
                            <span class="text-xl">📊</span> Export to Excel
                        </button>
                        <button class="px-4 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors">
                            <span class="text-xl">📄</span> Export to CSV
                        </button>
                    </div>
                </div>
                
                <!-- Data Table Placeholder -->
                <div class="overflow-x-auto">
                    <table id="wardDataTable" class="min-w-full">
                        <thead>
                            <tr class="bg-gray-50">
                                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Ward</th>
                                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">LGA</th>
                                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Risk Score</th>
                                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Category</th>
                                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Population</th>
                                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ITN Coverage</th>
                            </tr>
                        </thead>
                        <tbody class="bg-white divide-y divide-gray-200">
                            <!-- Table will be populated by JavaScript -->
                        </tbody>
                    </table>
                </div>
            </div>
        </section>
        
        <!-- Insights & Recommendations -->
        <section class="mb-12 animate-fade-in">
            <h2 class="text-3xl font-bold text-dark mb-8 flex items-center">
                <span class="w-2 h-8 bg-gradient-to-b from-indigo-500 to-purple-600 rounded mr-4"></span>
                AI-Powered Insights & Recommendations
            </h2>
            
            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <!-- Key Findings -->
                <div class="bg-gradient-to-br from-indigo-50 to-purple-50 rounded-2xl p-6 border border-indigo-200">
                    <h3 class="text-xl font-semibold mb-4 text-indigo-900 flex items-center">
                        <span class="text-2xl mr-3">🔍</span> Key Findings
                    </h3>
                    <ul class="space-y-3">
                        <li class="flex items-start">
                            <span class="text-indigo-500 mr-2 mt-1">&bull;</span>
                            <span class="text-gray-700">{risk_distribution.get('Very High Risk', 0) + risk_distribution.get('High Risk', 0)} wards identified as high-priority for immediate intervention</span>
                        </li>
                        <li class="flex items-start">
                            <span class="text-indigo-500 mr-2 mt-1">&bull;</span>
                            <span class="text-gray-700">Current allocation strategy covers {stats.get('coverage_percent', 0)}% of the population ({stats.get('population_covered', 0):,} people)</span>
                        </li>
                        <li class="flex items-start">
                            <span class="text-indigo-500 mr-2 mt-1">&bull;</span>
                            <span class="text-gray-700">Optimal distribution achieved with {stats.get('fully_covered_wards', 0)} wards receiving full coverage</span>
                        </li>
                    </ul>
                </div>
                
                <!-- Recommendations -->
                <div class="bg-gradient-to-br from-emerald-50 to-teal-50 rounded-2xl p-6 border border-emerald-200">
                    <h3 class="text-xl font-semibold mb-4 text-emerald-900 flex items-center">
                        <span class="text-2xl mr-3">💡</span> Strategic Recommendations
                    </h3>
                    <ul class="space-y-3">
{"".join([f'''
                        <li class="flex items-start">
                            <span class="text-emerald-500 mr-2 mt-1">&bull;</span>
                            <span class="text-gray-700">{rec}</span>
                        </li>''' for rec in recommendations[:3]])}
                    </ul>
                </div>
            </div>
            
            <!-- Action Plan -->
            <div class="bg-white rounded-2xl shadow-lg p-6 mt-6 card-hover">
                <h3 class="text-xl font-semibold mb-4 text-gray-800 flex items-center">
                    <span class="text-2xl mr-3">📋</span> Recommended Action Plan
                </h3>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div class="bg-red-50 rounded-lg p-4 border-l-4 border-red-500">
                        <h4 class="font-semibold text-red-900 mb-2">Immediate (0-3 months)</h4>
                        <ul class="text-sm text-gray-700 space-y-1">
                            <li>&bull; Distribute ITNs to high-risk wards</li>
                            <li>&bull; Launch community education campaigns</li>
                            <li>&bull; Establish monitoring systems</li>
                        </ul>
                    </div>
                    <div class="bg-yellow-50 rounded-lg p-4 border-l-4 border-yellow-500">
                        <h4 class="font-semibold text-yellow-900 mb-2">Short-term (3-6 months)</h4>
                        <ul class="text-sm text-gray-700 space-y-1">
                            <li>&bull; Implement IRS in gap areas</li>
                            <li>&bull; Strengthen health facility capacity</li>
                            <li>&bull; Conduct usage surveys</li>
                        </ul>
                    </div>
                    <div class="bg-green-50 rounded-lg p-4 border-l-4 border-green-500">
                        <h4 class="font-semibold text-green-900 mb-2">Long-term (6-12 months)</h4>
                        <ul class="text-sm text-gray-700 space-y-1">
                            <li>&bull; Evaluate intervention impact</li>
                            <li>&bull; Plan next distribution cycle</li>
                            <li>&bull; Scale successful strategies</li>
                        </ul>
                    </div>
                </div>
            </div>
        </section>
        
        <!-- Footer -->
        <footer class="mt-16 py-8 border-t border-gray-200 text-center text-gray-600">
            <p class="mb-2">Generated by <strong>ChatMRPT</strong> - Malaria Risk Prioritization Tool</p>
            <p class="text-sm">© {export_data['export_date'].strftime('%Y')} | Powered by Advanced AI Analytics</p>
            <div class="mt-4 no-print">
                <button onclick="window.print()" class="px-6 py-2 bg-gray-800 text-white rounded-lg hover:bg-gray-900 transition-colors">
                    <span class="text-xl mr-2">🖨️</span> Print Dashboard
                </button>
            </div>
        </footer>
    </div>
    
    <!-- JavaScript for interactivity -->
    <script>
        // Risk distribution data
        const riskData = {json.dumps(risk_distribution)};
        const topWardsData = {json.dumps(top_wards_data)};
        
        // Create risk distribution donut chart
        const riskDistributionTrace = {{
            labels: Object.keys(riskData),
            values: Object.values(riskData),
            type: 'pie',
            hole: 0.4,
            marker: {{
                colors: ['#DC2626', '#F59E0B', '#3B82F6', '#10B981']
            }},
            textinfo: 'label+percent',
            textposition: 'outside'
        }};
        
        const riskDistributionLayout = {{
            showlegend: true,
            margin: {{t: 20, b: 20, l: 20, r: 20}},
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)'
        }};
        
        Plotly.newPlot('riskDistributionChart', [riskDistributionTrace], riskDistributionLayout, {{responsive: true}});
        
        // Create top wards horizontal bar chart
        const topWardsTrace = {{
            x: topWardsData.map(d => d.score).reverse(),
            y: topWardsData.map(d => d.ward).reverse(),
            type: 'bar',
            orientation: 'h',
            marker: {{
                color: topWardsData.map((d, i) => {{
                    const intensity = 1 - (i / topWardsData.length);
                    return `rgba(220, 38, 38, ${{intensity}})`;
                }}).reverse()
            }},
            text: topWardsData.map(d => d.score.toFixed(3)).reverse(),
            textposition: 'outside'
        }};
        
        const topWardsLayout = {{
            margin: {{t: 20, b: 40, l: 150, r: 50}},
            xaxis: {{
                title: 'Risk Score',
                range: [0, Math.max(...topWardsData.map(d => d.score)) * 1.1]
            }},
            yaxis: {{
                title: ''
            }},
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)'
        }};
        
        Plotly.newPlot('topWardsChart', [topWardsTrace], topWardsLayout, {{responsive: true}});
        
        // Create coverage gauge chart
        const coverageTrace = {{
            type: "indicator",
            mode: "gauge+number+delta",
            value: {stats.get('coverage_percent', 0)},
            title: {{ text: "Population Coverage %" }},
            delta: {{ reference: 80, increasing: {{ color: "#10B981" }} }},
            gauge: {{
                axis: {{ range: [null, 100] }},
                bar: {{ color: "#6366F1" }},
                steps: [
                    {{ range: [0, 25], color: "#FEE2E2" }},
                    {{ range: [25, 50], color: "#FED7AA" }},
                    {{ range: [50, 75], color: "#DBEAFE" }},
                    {{ range: [75, 100], color: "#D1FAE5" }}
                ],
                threshold: {{
                    line: {{ color: "red", width: 4 }},
                    thickness: 0.75,
                    value: 90
                }}
            }}
        }};
        
        const coverageLayout = {{
            margin: {{ t: 50, r: 50, l: 50, b: 50 }},
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)'
        }};
        
        Plotly.newPlot('coverageChart', [coverageTrace], coverageLayout, {{responsive: true}});
        
        // Tab functionality
        function showMap(mapType) {{
            // Update tab styling
            document.querySelectorAll('.tab').forEach(tab => {{
                tab.classList.remove('tab-active', 'bg-gradient-to-r', 'from-primary', 'to-indigo-600', 'text-white');
                tab.classList.add('hover:bg-gray-100');
            }});
            event.target.classList.add('tab-active');
            event.target.classList.remove('hover:bg-gray-100');
            
            // Update map content (placeholder)
            const mapContainer = document.getElementById('mapContainer');
            mapContainer.innerHTML = `
                <div class="absolute inset-0 flex items-center justify-center">
                    <div class="text-center">
                        <div class="loading mb-4"></div>
                        <p class="text-gray-600">Loading ${{mapType}} map...</p>
                    </div>
                </div>
            `;
            
            // Simulate loading
            setTimeout(() => {{
                mapContainer.innerHTML = `
                    <div class="absolute inset-0 flex items-center justify-center">
                        <div class="text-center">
                            <div class="text-6xl mb-4">${{mapType === 'risk' ? '🗺️' : mapType === 'itn' ? '🛏️' : '📊'}}</div>
                            <p class="text-gray-600">${{mapType.toUpperCase()}} map visualization</p>
                            <p class="text-sm text-gray-500 mt-2">Full interactive map available in exported files</p>
                        </div>
                    </div>
                `;
            }}, 1000);
        }}
        
        // Initialize DataTable
        $(document).ready(function() {{
            // Actual ward data from analysis
            const wardData = {ward_table_json};
            
            $('#wardDataTable').DataTable({{
                data: wardData,
                pageLength: 10,
                responsive: true,
                dom: 'Bfrtip',
                buttons: ['copy', 'csv', 'excel', 'pdf', 'print']
            }});
        }});
        
        // Animate counters on page load
        function animateCounters() {{
            const counters = document.querySelectorAll('.counter');
            counters.forEach(counter => {{
                const target = parseInt(counter.innerText.replace(/,/g, ''));
                const duration = 2000;
                const increment = target / (duration / 16);
                let current = 0;
                
                const updateCounter = () => {{
                    current += increment;
                    if (current < target) {{
                        counter.innerText = Math.floor(current).toLocaleString();
                        requestAnimationFrame(updateCounter);
                    }} else {{
                        counter.innerText = target.toLocaleString();
                    }}
                }};
                
                updateCounter();
            }});
        }}
        
        // Trigger animations when page loads
        window.addEventListener('load', () => {{
            animateCounters();
        }});
        
        // Smooth scroll for navigation
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {{
            anchor.addEventListener('click', function (e) {{
                e.preventDefault();
                document.querySelector(this.getAttribute('href')).scrollIntoView({{
                    behavior: 'smooth'
                }});
            }});
        }});
    </script>
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