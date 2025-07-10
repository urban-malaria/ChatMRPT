"""
Reporting and Context Module - Result Summary and Context Generation

This module handles result summarization, context generation for analysis,
and report formatting functionality.
Extracted from the monolithic DataHandler class as part of Phase 5 refactoring.

Functions:
- ReportGenerator: Main reporting functionality
- Analysis context generation
- Result summary and formatting
- Export capabilities
"""

import os
import json
import logging
import pandas as pd
import geopandas as gpd
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

# Set up logging
from app.services.variable_resolution_service import variable_resolver
logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Handles report generation, context provision, and result summarization
    """
    
    def __init__(self, session_folder: str, interaction_logger=None):
        """
        Initialize report generator
        
        Args:
            session_folder: Path to session folder for saving reports
            interaction_logger: Optional interaction logger
        """
        self.session_folder = session_folder
        self.interaction_logger = interaction_logger
        self.logger = logging.getLogger(__name__)
        
        # Ensure session folder exists
        os.makedirs(self.session_folder, exist_ok=True)
    
    def generate_analysis_summary(self, analysis_results: Dict[str, Any],
                                 csv_data: Optional[pd.DataFrame] = None,
                                 shapefile_data: Optional[gpd.GeoDataFrame] = None) -> Dict[str, Any]:
        """
        Generate comprehensive analysis summary
        
        Args:
            analysis_results: Results from complete analysis
            csv_data: Original CSV data
            shapefile_data: Optional shapefile data
            
        Returns:
            Dictionary with comprehensive analysis summary
        """
        try:
            summary = {
                'analysis_date': datetime.now().isoformat(),
                'status': 'success',
                'data_overview': {},
                'analysis_pipeline': {},
                'vulnerability_assessment': {},
                'recommendations': []
            }
            
            # Data overview
            if csv_data is not None:
                summary['data_overview'] = {
                    'total_wards': len(csv_data),
                    'total_variables': len(csv_data.columns),
                    'has_geographic_data': shapefile_data is not None,
                    'data_completeness': self._calculate_data_completeness(csv_data)
                }
            
            # Analysis pipeline summary
            if 'composite_variables' in analysis_results:
                summary['analysis_pipeline'] = {
                    'variables_used': len(analysis_results.get('composite_variables', [])),
                    'variable_names': analysis_results.get('composite_variables', []),
                    'cleaning_methods': analysis_results.get('na_handling_methods', []),
                    'normalization_applied': 'normalized_data' in analysis_results,
                    'scoring_method': analysis_results.get('scoring_method', 'mean')
                }
            
            # Vulnerability assessment summary - Fixed to handle None properly
            vulnerability_rankings = analysis_results.get('vulnerability_rankings')
            if vulnerability_rankings is not None and not vulnerability_rankings.empty:
                vulnerability_summary = self._generate_vulnerability_summary(vulnerability_rankings)
                summary['vulnerability_assessment'] = vulnerability_summary
            else:
                summary['vulnerability_assessment'] = {
                    'status': 'not_available',
                    'message': 'No vulnerability rankings generated'
                }
            
            # Generate recommendations
            summary['recommendations'] = self._generate_recommendations(analysis_results, csv_data)
            
            # Save summary to file
            summary_path = os.path.join(self.session_folder, 'analysis_summary.json')
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            summary['summary_file'] = summary_path
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating analysis summary: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error generating analysis summary: {str(e)}'
            }
    
    def generate_context_for_analysis(self, csv_data: pd.DataFrame,
                                     analysis_type: str = 'vulnerability',
                                     shapefile_data: Optional[gpd.GeoDataFrame] = None) -> Dict[str, Any]:
        """
        Generate comprehensive context for analysis
        
        Args:
            csv_data: CSV data to analyze
            analysis_type: Type of analysis ('vulnerability', 'urban', etc.)
            shapefile_data: Optional shapefile data
            
        Returns:
            Dictionary with analysis context
        """
        try:
            context = {
                'analysis_type': analysis_type,
                'timestamp': datetime.now().isoformat(),
                'data_characteristics': {},
                'variable_insights': {},
                'geographic_context': {},
                'analysis_scope': {}
            }
            
            # Data characteristics
            context['data_characteristics'] = {
                'total_observations': len(csv_data),
                'total_variables': len(csv_data.columns),
                'missing_data_summary': self._analyze_missing_data(csv_data),
                'data_types': {col: str(dtype) for col, dtype in csv_data.dtypes.items()}
            }
            
            # Variable insights
            numeric_vars = csv_data.select_dtypes(include=['number']).columns.tolist()
            if 'WardName' in numeric_vars:
                numeric_vars.remove('WardName')
            
            context['variable_insights'] = {
                'numeric_variables': len(numeric_vars),
                'variable_names': numeric_vars[:20],  # First 20 for brevity
                'potential_indicators': self._identify_potential_indicators(csv_data),
                'data_quality_flags': self._identify_quality_issues(csv_data)
            }
            
            # Geographic context
            if shapefile_data is not None:
                context['geographic_context'] = {
                    'has_geographic_data': True,
                    'coordinate_system': str(shapefile_data.crs),
                    'geometry_features': len(shapefile_data),
                    'spatial_coverage': self._assess_spatial_coverage(shapefile_data)
                }
            else:
                context['geographic_context'] = {
                    'has_geographic_data': False,
                    'note': 'No geographic data available for spatial analysis'
                }
            
            # Analysis scope
            context['analysis_scope'] = {
                'suitable_for_vulnerability_analysis': len(numeric_vars) >= 2,
                'recommended_approach': self._recommend_analysis_approach(csv_data, analysis_type),
                'expected_outputs': self._describe_expected_outputs(analysis_type),
                'limitations': self._identify_limitations(csv_data, shapefile_data)
            }
            
            # Save context to file
            context_path = os.path.join(self.session_folder, 'analysis_context.json')
            with open(context_path, 'w', encoding='utf-8') as f:
                json.dump(context, f, indent=2, ensure_ascii=False)
            
            context['context_file'] = context_path
            
            return context
            
        except Exception as e:
            self.logger.error(f"Error generating analysis context: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error generating analysis context: {str(e)}'
            }
    
    def format_vulnerability_results(self, vulnerability_rankings: pd.DataFrame,
                                   composite_variables: Optional[List[str]] = None,
                                   top_n: int = 10) -> Dict[str, Any]:
        """
        Format vulnerability analysis results for presentation
        
        Args:
            vulnerability_rankings: DataFrame with vulnerability rankings
            composite_variables: Variables used in analysis
            top_n: Number of top/bottom wards to highlight
            
        Returns:
            Formatted results dictionary
        """
        try:
            if vulnerability_rankings is None or vulnerability_rankings.empty:
                return {
                    'status': 'error',
                    'message': 'No vulnerability rankings provided'
                }
            
            formatted_results = {
                'status': 'success',
                'overview': {},
                'top_vulnerable_wards': [],
                'least_vulnerable_wards': [],
                'category_distribution': {},
                'key_insights': []
            }
            
            # Overview
            formatted_results['overview'] = {
                'total_wards_analyzed': len(vulnerability_rankings),
                'variables_in_analysis': composite_variables or [],
                'analysis_date': datetime.now().isoformat()
            }
            
            # Sort by vulnerability score (assuming higher score = more vulnerable)
            if 'composite_vulnerability_score' in vulnerability_rankings.columns:
                sorted_rankings = vulnerability_rankings.sort_values(
                    'composite_vulnerability_score', 
                    ascending=False
                )
                
                # Top vulnerable wards
                top_wards = sorted_rankings.head(top_n)
                formatted_results['top_vulnerable_wards'] = [
                    {
                        'rank': i + 1,
                        'ward_name': row['WardName'],
                        'vulnerability_score': round(row['composite_vulnerability_score'], 3),
                        'category': row.get('vulnerability_category', 'Unknown')
                    }
                    for i, (_, row) in enumerate(top_wards.iterrows())
                ]
                
                # Least vulnerable wards
                bottom_wards = sorted_rankings.tail(top_n)
                formatted_results['least_vulnerable_wards'] = [
                    {
                        'rank': len(vulnerability_rankings) - top_n + i + 1,
                        'ward_name': row['WardName'],
                        'vulnerability_score': round(row['composite_vulnerability_score'], 3),
                        'category': row.get('vulnerability_category', 'Unknown')
                    }
                    for i, (_, row) in enumerate(bottom_wards.iterrows())
                ]
            
            # Category distribution
            if 'vulnerability_category' in vulnerability_rankings.columns:
                category_counts = vulnerability_rankings['vulnerability_category'].value_counts()
                formatted_results['category_distribution'] = category_counts.to_dict()
            
            # Generate key insights
            formatted_results['key_insights'] = self._generate_key_insights(
                vulnerability_rankings, composite_variables
            )
            
            # Save formatted results
            results_path = os.path.join(self.session_folder, 'formatted_vulnerability_results.json')
            with open(results_path, 'w', encoding='utf-8') as f:
                json.dump(formatted_results, f, indent=2, ensure_ascii=False)
            
            formatted_results['results_file'] = results_path
            
            return formatted_results
            
        except Exception as e:
            self.logger.error(f"Error formatting vulnerability results: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error formatting vulnerability results: {str(e)}'
            }
    
    def export_analysis_report(self, analysis_results: Dict[str, Any],
                              format_type: str = 'comprehensive') -> Dict[str, Any]:
        """
        Export comprehensive analysis report
        
        Args:
            analysis_results: Complete analysis results
            format_type: Type of report ('comprehensive', 'summary', 'technical')
            
        Returns:
            Export status and file paths
        """
        try:
            export_results = {
                'status': 'success',
                'format': format_type,
                'files_created': [],
                'export_timestamp': datetime.now().isoformat()
            }
            
            # Create export directory
            export_dir = os.path.join(self.session_folder, 'exports')
            os.makedirs(export_dir, exist_ok=True)
            
            # Check if vulnerability rankings exist before export
            vulnerability_rankings = analysis_results.get('vulnerability_rankings')
            if vulnerability_rankings is None or vulnerability_rankings.empty:
                return {
                    'status': 'error',
                    'message': 'No vulnerability rankings available for export'
                }
            
            # Export based on format type
            if format_type == 'comprehensive':
                # Export all data files
                export_files = self._export_comprehensive_report(analysis_results, export_dir)
            elif format_type == 'summary':
                # Export summary only
                export_files = self._export_summary_report(analysis_results, export_dir)
            elif format_type == 'technical':
                # Export technical details
                export_files = self._export_technical_report(analysis_results, export_dir)
            else:
                return {
                    'status': 'error',
                    'message': f'Unknown format type: {format_type}'
                }
            
            export_results['files_created'] = export_files
            
            return export_results
            
        except Exception as e:
            self.logger.error(f"Error exporting analysis report: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error exporting analysis report: {str(e)}'
            }
    
    def _calculate_data_completeness(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Calculate data completeness metrics
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Completeness metrics
        """
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isna().sum().sum()
        
        return {
            'overall_completeness': round((total_cells - missing_cells) / total_cells, 3),
            'missing_cell_count': int(missing_cells),
            'total_cell_count': int(total_cells)
        }
    
    def _generate_vulnerability_summary(self, vulnerability_rankings: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate vulnerability assessment summary - Fixed to handle None properly
        """
        summary = {}
        
        # Handle None case
        if vulnerability_rankings is None:
            return {
                'status': 'not_available',
                'message': 'No vulnerability rankings data available'
            }
        
        # Handle empty DataFrame
        if vulnerability_rankings.empty:
            return {
                'status': 'not_available',
                'message': 'Vulnerability rankings data is empty'
            }
        
        # Generate summary if data exists
        if 'composite_vulnerability_score' in vulnerability_rankings.columns:
            scores = vulnerability_rankings['composite_vulnerability_score']
            summary['score_statistics'] = {
                'mean': round(scores.mean(), 3),
                'median': round(scores.median(), 3),
                'std_dev': round(scores.std(), 3),
                'min': round(scores.min(), 3),
                'max': round(scores.max(), 3)
            }
        
        if 'vulnerability_category' in vulnerability_rankings.columns:
            category_counts = vulnerability_rankings['vulnerability_category'].value_counts()
            summary['category_distribution'] = category_counts.to_dict()
            summary['most_common_category'] = category_counts.index[0]
        
        return summary
    
    def _generate_recommendations(self, analysis_results: Dict[str, Any], 
                                csv_data: Optional[pd.DataFrame]) -> List[str]:
        """
        Generate analysis recommendations
        """
        recommendations = []
        
        # Check data quality
        if csv_data is not None:
            missing_ratio = csv_data.isna().sum().sum() / (csv_data.shape[0] * csv_data.shape[1])
            if missing_ratio > 0.1:
                recommendations.append("Consider addressing missing data for more robust analysis")
        
        # Check variable count
        if 'composite_variables' in analysis_results:
            var_count = len(analysis_results['composite_variables'])
            if var_count < 5:
                recommendations.append("Consider including more variables for comprehensive vulnerability assessment")
        
        # Add general recommendations
        recommendations.extend([
            "Validate results with local knowledge and field observations",
            "Consider temporal analysis if historical data is available",
            "Integrate results with local development planning processes"
        ])
        
        return recommendations
    
    def _analyze_missing_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze missing data patterns
        """
        missing_counts = df.isna().sum()
        total_missing = missing_counts.sum()
        
        return {
            'total_missing_values': int(total_missing),
            'columns_with_missing': missing_counts[missing_counts > 0].to_dict(),
            'completeness_by_column': ((len(df) - missing_counts) / len(df)).to_dict()
        }
    
    def _identify_potential_indicators(self, df: pd.DataFrame) -> List[str]:
        """
        Identify potential vulnerability indicators
        """
        indicators = []
        
        # Common vulnerability indicator patterns
        indicator_patterns = [
            'poverty', 'income', 'education', 'health', 'infrastructure',
            'unemployment', 'population', 'density', 'access', 'water',
            'sanitation', 'electricity', 'housing'
        ]
        
        for col in df.columns:
            col_lower = col.lower()
            for pattern in indicator_patterns:
                if pattern in col_lower:
                    indicators.append(col)
                    break
        
        return indicators[:10]  # Limit to first 10 matches
    
    def _identify_quality_issues(self, df: pd.DataFrame) -> List[str]:
        """
        Identify data quality issues
        """
        issues = []
        
        # Check for columns with high missing values
        missing_ratios = df.isna().sum() / len(df)
        high_missing = missing_ratios[missing_ratios > 0.5]
        if not high_missing.empty:
            issues.append(f"{len(high_missing)} columns have >50% missing values")
        
        # Check for potential duplicate wards
        if 'WardName' in df.columns:
            duplicates = df['WardName'].duplicated().sum()
            if duplicates > 0:
                issues.append(f"{duplicates} potential duplicate ward names")
        
        return issues
    
    def _assess_spatial_coverage(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """
        Assess spatial coverage of geographic data
        """
        try:
            bounds = gdf.total_bounds
            return {
                'bounding_box': {
                    'min_x': float(bounds[0]),
                    'min_y': float(bounds[1]),
                    'max_x': float(bounds[2]),
                    'max_y': float(bounds[3])
                },
                'total_area': float(gdf.geometry.area.sum()) if hasattr(gdf.geometry, 'area') else None
            }
        except Exception:
            return {'note': 'Could not calculate spatial coverage'}
    
    def _recommend_analysis_approach(self, df: pd.DataFrame, analysis_type: str) -> str:
        """
        Recommend analysis approach based on data characteristics
        """
        numeric_vars = len(df.select_dtypes(include=['number']).columns)
        
        if analysis_type == 'vulnerability':
            if numeric_vars >= 5:
                return "Multi-dimensional vulnerability assessment with composite scoring"
            elif numeric_vars >= 2:
                return "Basic vulnerability assessment with available indicators"
            else:
                return "Limited analysis - consider additional data sources"
        
        return "Standard analysis approach"
    
    def _describe_expected_outputs(self, analysis_type: str) -> List[str]:
        """
        Describe expected analysis outputs
        """
        if analysis_type == 'vulnerability':
            return [
                "Vulnerability rankings for all wards",
                "Composite vulnerability scores",
                "Vulnerability categories (High/Medium/Low)",
                "Variable contribution analysis",
                "Spatial vulnerability patterns (if geographic data available)"
            ]
        
        return ["Analysis results and summary statistics"]
    
    def _identify_limitations(self, df: pd.DataFrame, 
                            shapefile_data: Optional[gpd.GeoDataFrame]) -> List[str]:
        """
        Identify analysis limitations
        """
        limitations = []
        
        # Data size limitations
        if len(df) < 10:
            limitations.append("Small sample size may limit statistical validity")
        
        # Variable limitations
        numeric_vars = len(df.select_dtypes(include=['number']).columns)
        if numeric_vars < 3:
            limitations.append("Limited number of variables for comprehensive analysis")
        
        # Geographic limitations
        if shapefile_data is None:
            limitations.append("No geographic data available for spatial analysis")
        
        # Missing data limitations
        missing_ratio = df.isna().sum().sum() / (df.shape[0] * df.shape[1])
        if missing_ratio > 0.2:
            limitations.append("High levels of missing data may affect result reliability")
        
        return limitations
    
    def _generate_key_insights(self, vulnerability_rankings: pd.DataFrame,
                             composite_variables: Optional[List[str]]) -> List[str]:
        """
        Generate key insights from vulnerability analysis
        """
        insights = []
        
        # Score distribution insights
        if 'composite_vulnerability_score' in vulnerability_rankings.columns:
            scores = vulnerability_rankings['composite_vulnerability_score']
            high_vuln_count = (scores > scores.quantile(0.75)).sum()
            insights.append(f"{high_vuln_count} wards show high vulnerability (top 25%)")
        
        # Category insights
        if 'vulnerability_category' in vulnerability_rankings.columns:
            categories = vulnerability_rankings['vulnerability_category'].value_counts()
            top_category = categories.index[0]
            insights.append(f"Most wards fall into '{top_category}' vulnerability category")
        
        # Variable insights
        if composite_variables:
            insights.append(f"Analysis based on {len(composite_variables)} key indicators")
        
        return insights
    
    def _export_comprehensive_report(self, analysis_results: Dict[str, Any], 
                                   export_dir: str) -> List[str]:
        """
        Export comprehensive analysis report - Fixed to handle None properly
        """
        files_created = []
        
        # Export main results - Fixed to check for None
        vulnerability_rankings = analysis_results.get('vulnerability_rankings')
        if vulnerability_rankings is not None and not vulnerability_rankings.empty:
            vuln_path = os.path.join(export_dir, 'vulnerability_rankings.csv')
            vulnerability_rankings.to_csv(vuln_path, index=False)
            files_created.append(vuln_path)
        
        # Export other data files
        for key, filename in [
            ('cleaned_data', 'cleaned_data.csv'),
            ('normalized_data', 'normalized_data.csv'),
        ]:
            if key in analysis_results and analysis_results[key] is not None:
                data = analysis_results[key]
                if hasattr(data, 'to_csv'):  # Check if it's a DataFrame
                    file_path = os.path.join(export_dir, filename)
                    data.to_csv(file_path, index=False)
                    files_created.append(file_path)
        
        return files_created
    
    def _export_summary_report(self, analysis_results: Dict[str, Any], 
                             export_dir: str) -> List[str]:
        """
        Export summary report only - Fixed to handle None properly
        """
        files_created = []
        
        # Export only the main vulnerability rankings - Fixed to check for None
        vulnerability_rankings = analysis_results.get('vulnerability_rankings')
        if vulnerability_rankings is not None and not vulnerability_rankings.empty:
            summary_path = os.path.join(export_dir, 'vulnerability_summary.csv')
            # Export only key columns
            key_columns = ['WardName', 'composite_vulnerability_score', 'vulnerability_category']
            available_columns = [col for col in key_columns if col in vulnerability_rankings.columns]
            
            if available_columns:  # Only export if we have valid columns
                vulnerability_rankings[available_columns].to_csv(summary_path, index=False)
                files_created.append(summary_path)
        
        return files_created
    
    def _export_technical_report(self, analysis_results: Dict[str, Any], 
                               export_dir: str) -> List[str]:
        """
        Export technical analysis report
        """
        files_created = []
        
        # Export technical metadata
        metadata = {
            'analysis_metadata': analysis_results.get('analysis_metadata', {}),
            'variable_relationships': analysis_results.get('variable_relationships', {}),
            'na_handling_methods': analysis_results.get('na_handling_methods', []),
            'composite_variables': analysis_results.get('composite_variables', [])
        }
        
        metadata_path = os.path.join(export_dir, 'technical_metadata.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        files_created.append(metadata_path)
        
        return files_created


# Convenience functions for backward compatibility
def generate_result_summary(analysis_results: Dict[str, Any], session_folder: str,
                          csv_data: Optional[pd.DataFrame] = None,
                          shapefile_data: Optional[gpd.GeoDataFrame] = None,
                          interaction_logger=None) -> Dict[str, Any]:
    """
    Convenience function to generate analysis result summary
    
    Args:
        analysis_results: Analysis results to summarize
        session_folder: Session folder path
        csv_data: Optional original CSV data
        shapefile_data: Optional shapefile data
        interaction_logger: Optional interaction logger
        
    Returns:
        Analysis summary dictionary
    """
    generator = ReportGenerator(session_folder, interaction_logger)
    return generator.generate_analysis_summary(analysis_results, csv_data, shapefile_data)


# Package information
__version__ = "1.0.0"
__all__ = [
    'ReportGenerator',
    'generate_result_summary'
]