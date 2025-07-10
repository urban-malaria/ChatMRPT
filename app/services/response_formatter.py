"""
Response Formatter Service for ChatMRPT

This service provides consistent markdown formatting for all ChatMRPT responses,
replacing the raw unformatted text that appears throughout the system.

Key features:
- Converts analysis results to properly formatted markdown
- Formats tables, lists, headers consistently
- Adds structure to raw LLM outputs
- Ensures all responses are user-friendly
"""

import logging
import re
from typing import Dict, List, Optional, Any, Union
import pandas as pd

logger = logging.getLogger(__name__)


class ResponseFormatter:
    """
    Service for formatting ChatMRPT responses into readable markdown.
    
    This replaces the raw unformatted responses that appear throughout
    the system with properly structured, user-friendly output.
    """
    
    def __init__(self):
        self.table_formats = {
            'rankings': self._format_rankings_table,
            'statistics': self._format_statistics_table,
            'comparison': self._format_comparison_table
        }
    
    def format_analysis_result(self, 
                             result: Dict[str, Any],
                             analysis_type: str = 'analysis') -> str:
        """
        Format analysis results into readable markdown.
        
        Args:
            result: Analysis result dictionary
            analysis_type: Type of analysis (composite, pca, custom)
            
        Returns:
            Formatted markdown string
        """
        if not result or not isinstance(result, dict):
            return "Analysis completed but no detailed results available."
        
        formatted_parts = []
        
        # Add title
        title = self._create_title(result, analysis_type)
        if title:
            formatted_parts.append(title)
        
        # Add summary section
        summary = self._extract_summary(result)
        if summary:
            formatted_parts.append(f"## Summary\n\n{summary}")
        
        # Add results section
        results = self._format_results_section(result, analysis_type)
        if results:
            formatted_parts.append(results)
        
        # Add rankings if available
        rankings = self._format_rankings_section(result)
        if rankings:
            formatted_parts.append(rankings)
        
        # Add recommendations
        recommendations = self._format_recommendations(result)
        if recommendations:
            formatted_parts.append(recommendations)
        
        # Add metadata
        metadata = self._format_metadata(result)
        if metadata:
            formatted_parts.append(metadata)
        
        return "\n\n".join(formatted_parts)
    
    def format_error_message(self, 
                           error_msg: str,
                           context: str = "",
                           suggestions: List[str] = None) -> str:
        """
        Format error messages with helpful structure.
        
        Args:
            error_msg: The error message
            context: Additional context
            suggestions: List of suggestions
            
        Returns:
            Formatted error message
        """
        formatted_parts = ["❌ **Error**"]
        
        # Add main error
        formatted_parts.append(f"{error_msg}")
        
        # Add context if provided
        if context:
            formatted_parts.append(f"\n**Context:** {context}")
        
        # Add suggestions if provided
        if suggestions:
            formatted_parts.append("\n**Suggestions:**")
            for i, suggestion in enumerate(suggestions, 1):
                formatted_parts.append(f"{i}. {suggestion}")
        
        return "\n".join(formatted_parts)
    
    def format_variable_list(self, 
                           variables: List[str],
                           title: str = "Available Variables",
                           max_items: int = 20) -> str:
        """
        Format a list of variables in a readable way.
        
        Args:
            variables: List of variable names
            title: Title for the list
            max_items: Maximum items to show
            
        Returns:
            Formatted variable list
        """
        if not variables:
            return f"**{title}:** None available"
        
        formatted_parts = [f"**{title}:**"]
        
        # Group variables by category if possible
        categorized = self._categorize_variables(variables)
        
        if len(categorized) > 1:
            # Show by category
            for category, vars_in_category in categorized.items():
                if vars_in_category:
                    formatted_parts.append(f"\n*{category}:*")
                    for var in vars_in_category[:max_items//len(categorized)]:
                        formatted_parts.append(f"  • {var}")
        else:
            # Show as simple list
            for var in variables[:max_items]:
                formatted_parts.append(f"  • {var}")
        
        if len(variables) > max_items:
            formatted_parts.append(f"  ... and {len(variables) - max_items} more")
        
        return "\n".join(formatted_parts)
    
    def format_statistics(self, 
                        stats: Dict[str, Any],
                        variable_name: str = None) -> str:
        """
        Format statistical information.
        
        Args:
            stats: Statistics dictionary
            variable_name: Name of the variable
            
        Returns:
            Formatted statistics
        """
        if not stats:
            return ""
        
        title = f"**{variable_name} Statistics**" if variable_name else "**Statistics**"
        formatted_parts = [title]
        
        # Common statistics
        stat_mappings = {
            'count': 'Records',
            'mean': 'Average',
            'std': 'Standard Deviation',
            'min': 'Minimum',
            'max': 'Maximum',
            'median': 'Median',
            'missing': 'Missing Values'
        }
        
        for key, label in stat_mappings.items():
            if key in stats:
                value = stats[key]
                if isinstance(value, float):
                    if key in ['mean', 'std', 'min', 'max', 'median']:
                        formatted_parts.append(f"• **{label}:** {value:.2f}")
                    else:
                        formatted_parts.append(f"• **{label}:** {value}")
                else:
                    formatted_parts.append(f"• **{label}:** {value}")
        
        # Add range if min/max available
        if 'min' in stats and 'max' in stats:
            range_val = f"{stats['min']:.2f} to {stats['max']:.2f}"
            formatted_parts.append(f"• **Range:** {range_val}")
        
        return "\n".join(formatted_parts)
    
    def format_rankings_table(self, 
                            rankings: List[Dict[str, Any]],
                            title: str = "Rankings",
                            top_n: int = 10) -> str:
        """
        Format rankings data as a markdown table.
        
        Args:
            rankings: List of ranking dictionaries
            title: Table title
            top_n: Number of top items to show
            
        Returns:
            Formatted markdown table
        """
        if not rankings:
            return f"**{title}:** No data available"
        
        # Take top N
        top_rankings = rankings[:top_n]
        
        formatted_parts = [f"### {title}"]
        
        # Create table header
        if top_rankings:
            headers = list(top_rankings[0].keys())
            # Common header replacements
            header_mappings = {
                'WardName': 'Ward',
                'ward_name': 'Ward',
                'composite_score': 'Risk Score',
                'pca_score': 'PCA Score',
                'rank': 'Rank'
            }
            
            display_headers = [header_mappings.get(h, h.title()) for h in headers]
            
            # Create table
            table_lines = [
                "| " + " | ".join(display_headers) + " |",
                "| " + " | ".join(["---"] * len(display_headers)) + " |"
            ]
            
            for item in top_rankings:
                row_values = []
                for header in headers:
                    value = item.get(header, '')
                    if isinstance(value, float):
                        if header.endswith('_score') or header.endswith('score'):
                            row_values.append(f"{value:.3f}")
                        else:
                            row_values.append(f"{value:.2f}")
                    else:
                        row_values.append(str(value))
                
                table_lines.append("| " + " | ".join(row_values) + " |")
            
            formatted_parts.extend(table_lines)
        
        return "\n".join(formatted_parts)
    
    def format_method_comparison(self, 
                               composite_results: Dict[str, Any],
                               pca_results: Dict[str, Any]) -> str:
        """
        Format comparison between analysis methods.
        
        Args:
            composite_results: Composite analysis results
            pca_results: PCA analysis results
            
        Returns:
            Formatted comparison
        """
        formatted_parts = ["## Method Comparison"]
        
        # Compare top wards
        if composite_results.get('rankings') and pca_results.get('rankings'):
            comp_top = [w['WardName'] for w in composite_results['rankings'][:5]]
            pca_top = [w['WardName'] for w in pca_results['rankings'][:5]]
            
            formatted_parts.append("### Top 5 Highest Risk Wards")
            formatted_parts.append("**Composite Method:**")
            for i, ward in enumerate(comp_top, 1):
                formatted_parts.append(f"{i}. {ward}")
            
            formatted_parts.append("\n**PCA Method:**")
            for i, ward in enumerate(pca_top, 1):
                formatted_parts.append(f"{i}. {ward}")
            
            # Find overlap
            overlap = set(comp_top) & set(pca_top)
            if overlap:
                formatted_parts.append(f"\n**Consensus High-Risk Wards:** {', '.join(overlap)}")
        
        return "\n".join(formatted_parts)
    
    def _create_title(self, result: Dict[str, Any], analysis_type: str) -> str:
        """Create a title for the analysis result."""
        titles = {
            'composite': '# 🎯 Composite Risk Analysis Results',
            'pca': '# 📊 Principal Component Analysis Results',
            'complete': '# 📈 Comprehensive Malaria Risk Analysis',
            'custom': '# ⚙️ Custom Analysis Results'
        }
        
        return titles.get(analysis_type, '# 📊 Analysis Results')
    
    def _extract_summary(self, result: Dict[str, Any]) -> str:
        """Extract summary information from results."""
        summary_parts = []
        
        # Data summary
        if 'data' in result:
            data = result['data']
            if 'ward_count' in data:
                summary_parts.append(f"Analyzed **{data['ward_count']} wards**")
            if 'variable_count' in data:
                summary_parts.append(f"using **{data['variable_count']} variables**")
        
        # Method summary
        if 'methods_run' in result:
            methods = result['methods_run']
            if methods:
                method_names = ', '.join([m.replace('_', ' ').title() for m in methods])
                summary_parts.append(f"Methods: **{method_names}**")
        
        # Execution time
        if 'execution_time' in result:
            time_val = result['execution_time']
            if isinstance(time_val, (int, float)):
                summary_parts.append(f"Completed in **{time_val:.1f} seconds**")
        
        return " • ".join(summary_parts) if summary_parts else ""
    
    def _format_results_section(self, result: Dict[str, Any], analysis_type: str) -> str:
        """Format the main results section."""
        if 'message' in result and result['message']:
            # If there's a detailed message, use it
            message = result['message']
            if not message.startswith('#') and not message.startswith('##'):
                return f"## Results\n\n{message}"
            return message
        
        return ""
    
    def _format_rankings_section(self, result: Dict[str, Any]) -> str:
        """Format rankings section if available."""
        if 'data' in result and 'rankings' in result['data']:
            rankings = result['data']['rankings']
            if rankings:
                return self.format_rankings_table(rankings, "Top Risk Areas")
        
        return ""
    
    def _format_recommendations(self, result: Dict[str, Any]) -> str:
        """Format recommendations section."""
        recommendations = []
        
        # Extract recommendations from various sources
        if 'recommendations' in result:
            recommendations.extend(result['recommendations'])
        
        if 'data' in result and 'recommendations' in result['data']:
            recommendations.extend(result['data']['recommendations'])
        
        if recommendations:
            formatted_parts = ["## 💡 Recommendations"]
            for i, rec in enumerate(recommendations, 1):
                formatted_parts.append(f"{i}. {rec}")
            return "\n".join(formatted_parts)
        
        return ""
    
    def _format_metadata(self, result: Dict[str, Any]) -> str:
        """Format metadata section."""
        if 'metadata' in result:
            metadata = result['metadata']
            formatted_parts = ["## Technical Details"]
            
            # Variables used
            if 'variables_used' in metadata:
                vars_used = metadata['variables_used']
                if vars_used:
                    formatted_parts.append(f"**Variables Used:** {', '.join(vars_used)}")
            
            # Analysis parameters
            if 'parameters' in metadata:
                params = metadata['parameters']
                if params:
                    formatted_parts.append("**Parameters:**")
                    for key, value in params.items():
                        formatted_parts.append(f"  • {key.replace('_', ' ').title()}: {value}")
            
            return "\n".join(formatted_parts) if len(formatted_parts) > 1 else ""
        
        return ""
    
    def _categorize_variables(self, variables: List[str]) -> Dict[str, List[str]]:
        """Categorize variables by type."""
        categories = {
            'Environmental': [],
            'Health': [],
            'Demographic': [],
            'Geographic': [],
            'Other': []
        }
        
        for var in variables:
            var_lower = var.lower()
            if any(term in var_lower for term in ['temp', 'rain', 'humid', 'evi', 'ndvi', 'elevation', 'soil']):
                categories['Environmental'].append(var)
            elif any(term in var_lower for term in ['health', 'pfpr', 'malaria', 'tpr', 'mortality']):
                categories['Health'].append(var)
            elif any(term in var_lower for term in ['pop', 'urban', 'density', 'poverty', 'education']):
                categories['Demographic'].append(var)
            elif any(term in var_lower for term in ['distance', 'water', 'lat', 'lon', 'coord']):
                categories['Geographic'].append(var)
            else:
                categories['Other'].append(var)
        
        # Remove empty categories
        return {k: v for k, v in categories.items() if v}
    
    def _format_rankings_table(self, data: List[Dict], headers: List[str] = None) -> str:
        """Helper to format rankings as a table."""
        return self.format_rankings_table(data, "Rankings")
    
    def _format_statistics_table(self, data: Dict, title: str = "Statistics") -> str:
        """Helper to format statistics as a table."""
        return self.format_statistics(data)
    
    def _format_comparison_table(self, data: Dict, headers: List[str] = None) -> str:
        """Helper to format comparison data."""
        # Implementation for comparison tables
        return "Comparison data formatted"
    
    def format_tool_result(self, 
                          message: str, 
                          data: Optional[Dict[str, Any]] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Format tool execution results for consistent presentation.
        
        Args:
            message: The original tool result message
            data: Tool-specific data
            metadata: Execution metadata
            
        Returns:
            Formatted markdown string
        """
        if not message:
            return ""
            
        # Check if message is already well-formatted (starts with markdown headers)
        if message.strip().startswith(('#', '##', '###', '####')):
            return message
            
        # Simple formatting for non-markdown messages
        formatted_parts = []
        
        # Add the main message
        if message:
            formatted_parts.append(message)
        
        # Add data summary if available
        if data and isinstance(data, dict):
            data_summary = self._format_data_summary(data)
            if data_summary:
                formatted_parts.append(f"\n{data_summary}")
        
        # Add metadata if available
        if metadata and isinstance(metadata, dict):
            metadata_summary = self._format_metadata_summary(metadata)
            if metadata_summary:
                formatted_parts.append(f"\n{metadata_summary}")
        
        return "\n".join(formatted_parts)
    
    def _format_data_summary(self, data: Dict[str, Any]) -> str:
        """Format data summary for tool results."""
        summary_parts = []
        
        # Common data fields
        if 'ward_count' in data:
            summary_parts.append(f"**Wards analyzed:** {data['ward_count']}")
        if 'variable_count' in data:
            summary_parts.append(f"**Variables used:** {data['variable_count']}")
        if 'total_wards' in data:
            summary_parts.append(f"**Total wards:** {data['total_wards']}")
        if 'charts_created' in data:
            summary_parts.append(f"**Charts created:** {data['charts_created']}")
        
        return " • ".join(summary_parts) if summary_parts else ""
    
    def _format_metadata_summary(self, metadata: Dict[str, Any]) -> str:
        """Format metadata summary for tool results."""
        summary_parts = []
        
        # Common metadata fields
        if 'execution_time' in metadata:
            time_val = metadata['execution_time']
            if isinstance(time_val, (int, float)):
                summary_parts.append(f"**Execution time:** {time_val:.2f}s")
        
        if 'tool_category' in metadata:
            summary_parts.append(f"**Category:** {metadata['tool_category']}")
        
        return " • ".join(summary_parts) if summary_parts else ""


# Global instance for easy access
response_formatter = ResponseFormatter()


# Convenience functions
def format_analysis_result(result: Dict[str, Any], 
                         analysis_type: str = 'analysis') -> str:
    """Format analysis results using the global formatter."""
    return response_formatter.format_analysis_result(result, analysis_type)


def format_error_message(error_msg: str,
                        context: str = "",
                        suggestions: List[str] = None) -> str:
    """Format error messages using the global formatter."""
    return response_formatter.format_error_message(error_msg, context, suggestions)


def format_variable_list(variables: List[str],
                        title: str = "Available Variables",
                        max_items: int = 20) -> str:
    """Format variable lists using the global formatter."""
    return response_formatter.format_variable_list(variables, title, max_items)


def format_statistics(stats: Dict[str, Any],
                     variable_name: str = None) -> str:
    """Format statistics using the global formatter."""
    return response_formatter.format_statistics(stats, variable_name)