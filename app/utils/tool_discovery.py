"""
Tool Discovery Helper Module

Helps users discover available tools based on their current context.
Shows what tools are available and suggests relevant ones.
"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class ToolDiscoveryHelper:
    """Helper class for tool discovery and suggestions."""

    def __init__(self):
        """Initialize the tool discovery helper."""
        # Define tools by category
        self.tool_categories = {
            'data_upload': {
                'name': 'Data Upload & Management',
                'icon': '📎',
                'tools': [
                    {
                        'name': 'Upload CSV/Excel',
                        'command': 'upload data',
                        'description': 'Upload ward-level data file',
                        'requires': []
                    },
                    {
                        'name': 'Upload Shapefile',
                        'command': 'upload shapefile',
                        'description': 'Add geographic boundaries',
                        'requires': []
                    },
                    {
                        'name': 'Load Sample Data',
                        'command': 'load sample data',
                        'description': 'Use example dataset',
                        'requires': []
                    },
                    {
                        'name': 'Validate Data',
                        'command': 'check my data',
                        'description': 'Validate data format',
                        'requires': ['data_uploaded']
                    }
                ]
            },
            'analysis': {
                'name': 'Risk Analysis',
                'icon': '🔬',
                'tools': [
                    {
                        'name': 'Run Malaria Risk Analysis',
                        'command': 'analyze malaria risk',
                        'description': 'Calculate vulnerability scores',
                        'requires': ['data_uploaded']
                    },
                    {
                        'name': 'TPR Analysis',
                        'command': 'calculate TPR',
                        'description': 'Test Positivity Rate calculation',
                        'requires': ['tpr_data_uploaded']
                    },
                    {
                        'name': 'Data Quality Check',
                        'command': 'check data quality',
                        'description': 'Assess data completeness',
                        'requires': ['data_uploaded']
                    },
                    {
                        'name': 'Statistical Analysis',
                        'command': 'run statistical analysis',
                        'description': 'Detailed statistical metrics',
                        'requires': ['data_uploaded']
                    }
                ]
            },
            'visualization': {
                'name': 'Visualizations & Maps',
                'icon': '📊',
                'tools': [
                    {
                        'name': 'Vulnerability Map',
                        'command': 'create vulnerability map',
                        'description': 'Interactive risk map',
                        'requires': ['analysis_complete']
                    },
                    {
                        'name': 'Variable Distribution',
                        'command': 'plot [variable] distribution',
                        'description': 'Map any variable spatially',
                        'requires': ['data_uploaded']
                    },
                    {
                        'name': 'Create Histogram',
                        'command': 'create histogram',
                        'description': 'Frequency distribution chart',
                        'requires': ['data_uploaded']
                    },
                    {
                        'name': 'Scatter Plot',
                        'command': 'create scatter plot',
                        'description': 'Compare two variables',
                        'requires': ['data_uploaded']
                    },
                    {
                        'name': 'Correlation Heatmap',
                        'command': 'show correlation heatmap',
                        'description': 'Variable relationships',
                        'requires': ['data_uploaded']
                    },
                    {
                        'name': 'Box Plot',
                        'command': 'create box plot',
                        'description': 'Statistical distributions',
                        'requires': ['data_uploaded']
                    }
                ]
            },
            'queries': {
                'name': 'Data Queries & Rankings',
                'icon': '🔍',
                'tools': [
                    {
                        'name': 'Top N Wards',
                        'command': 'show top 10 wards',
                        'description': 'Highest risk wards',
                        'requires': ['analysis_complete']
                    },
                    {
                        'name': 'Ward Details',
                        'command': 'tell me about [ward name]',
                        'description': 'Specific ward information',
                        'requires': ['data_uploaded']
                    },
                    {
                        'name': 'Filter Wards',
                        'command': 'show wards with TPR > 20',
                        'description': 'Custom filtering',
                        'requires': ['data_uploaded']
                    },
                    {
                        'name': 'Compare Wards',
                        'command': 'compare ward A and ward B',
                        'description': 'Side-by-side comparison',
                        'requires': ['data_uploaded']
                    },
                    {
                        'name': 'Describe Data',
                        'command': 'describe my data',
                        'description': 'Column names and statistics',
                        'requires': ['data_uploaded']
                    }
                ]
            },
            'planning': {
                'name': 'Intervention Planning',
                'icon': '📋',
                'tools': [
                    {
                        'name': 'ITN Distribution',
                        'command': 'plan bed net distribution',
                        'description': 'Optimize net allocation',
                        'requires': ['analysis_complete']
                    },
                    {
                        'name': 'Intervention Map',
                        'command': 'create intervention map',
                        'description': 'Priority areas for action',
                        'requires': ['analysis_complete']
                    },
                    {
                        'name': 'Settlement Analysis',
                        'command': 'show settlement statistics',
                        'description': 'Building and population data',
                        'requires': ['shapefile_uploaded']
                    }
                ]
            },
            'export': {
                'name': 'Reports & Export',
                'icon': '📄',
                'tools': [
                    {
                        'name': 'Generate PDF Report',
                        'command': 'generate report',
                        'description': 'Comprehensive PDF summary',
                        'requires': ['analysis_complete']
                    },
                    {
                        'name': 'Export Results',
                        'command': 'export results',
                        'description': 'Download as CSV/Excel',
                        'requires': ['analysis_complete']
                    },
                    {
                        'name': 'Create Dashboard',
                        'command': 'create dashboard',
                        'description': 'Interactive HTML dashboard',
                        'requires': ['analysis_complete']
                    },
                    {
                        'name': 'Summary Statistics',
                        'command': 'generate summary',
                        'description': 'Key findings overview',
                        'requires': ['analysis_complete']
                    }
                ]
            }
        }

    def get_available_tools(self, session_data: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """
        Get all tools available based on current session state.

        Args:
            session_data: Current session data

        Returns:
            Dictionary of available tools by category
        """
        available = {}
        state = self._get_session_state(session_data)

        for category_key, category in self.tool_categories.items():
            available_in_category = []

            for tool in category['tools']:
                if self._is_tool_available(tool, state):
                    available_in_category.append(tool)

            if available_in_category:
                available[category_key] = {
                    'name': category['name'],
                    'icon': category['icon'],
                    'tools': available_in_category
                }

        return available

    def _get_session_state(self, session_data: Dict[str, Any]) -> Dict[str, bool]:
        """Extract relevant state flags from session data."""
        return {
            'data_uploaded': session_data.get('csv_loaded', False),
            'tpr_data_uploaded': session_data.get('tpr_data_loaded', False),
            'shapefile_uploaded': session_data.get('shapefile_loaded', False),
            'analysis_complete': session_data.get('analysis_complete', False),
            'tpr_complete': session_data.get('tpr_complete', False)
        }

    def _is_tool_available(self, tool: Dict, state: Dict[str, bool]) -> bool:
        """Check if a tool's requirements are met."""
        for requirement in tool.get('requires', []):
            if not state.get(requirement, False):
                return False
        return True

    def suggest_tools(self, session_data: Dict[str, Any], context: str = None) -> List[Dict]:
        """
        Suggest relevant tools based on context and state.

        Args:
            session_data: Current session data
            context: Optional context hint (e.g., 'visualization', 'analysis')

        Returns:
            List of suggested tools
        """
        suggestions = []
        state = self._get_session_state(session_data)

        # Priority suggestions based on workflow stage
        if not state['data_uploaded']:
            # Suggest data upload
            suggestions.extend([
                {
                    'command': 'upload data',
                    'reason': 'Start by uploading your data',
                    'priority': 'high'
                },
                {
                    'command': 'load sample data',
                    'reason': 'Or try with sample data first',
                    'priority': 'medium'
                }
            ])

        elif state['data_uploaded'] and not state['analysis_complete']:
            # Suggest analysis
            suggestions.append({
                'command': 'analyze malaria risk',
                'reason': 'Your data is ready for analysis',
                'priority': 'high'
            })

            if not state['shapefile_uploaded']:
                suggestions.append({
                    'command': 'upload shapefile',
                    'reason': 'Add shapefile for mapping capabilities',
                    'priority': 'medium'
                })

        elif state['analysis_complete']:
            # Suggest next steps
            suggestions.extend([
                {
                    'command': 'create vulnerability map',
                    'reason': 'Visualize your analysis results',
                    'priority': 'high'
                },
                {
                    'command': 'show top 10 wards',
                    'reason': 'See highest risk areas',
                    'priority': 'high'
                },
                {
                    'command': 'generate report',
                    'reason': 'Create comprehensive PDF report',
                    'priority': 'medium'
                },
                {
                    'command': 'plan bed net distribution',
                    'reason': 'Plan intervention strategies',
                    'priority': 'medium'
                }
            ])

        # Context-specific suggestions
        if context:
            if 'visual' in context.lower():
                viz_tools = self.tool_categories['visualization']['tools']
                for tool in viz_tools[:3]:  # Top 3 viz tools
                    if self._is_tool_available(tool, state):
                        suggestions.append({
                            'command': tool['command'],
                            'reason': tool['description'],
                            'priority': 'medium'
                        })

        return suggestions[:5]  # Return top 5 suggestions

    def format_discovery_message(self, available_tools: Dict, suggestions: List[Dict]) -> str:
        """
        Format tool discovery information for display.

        Args:
            available_tools: Available tools by category
            suggestions: Suggested tools

        Returns:
            Formatted message string
        """
        lines = []
        lines.append("## 🔧 Available Tools\n")

        if not available_tools:
            lines.append("No tools available yet. Start by uploading your data!\n")
            return "\n".join(lines)

        # Show available tools by category
        for category_key, category in available_tools.items():
            lines.append(f"### {category['icon']} {category['name']}")
            for tool in category['tools'][:5]:  # Show max 5 per category
                lines.append(f"• **{tool['name']}** - Say: `{tool['command']}`")
            if len(category['tools']) > 5:
                lines.append(f"  _...and {len(category['tools']) - 5} more_")
            lines.append("")

        # Show suggestions if any
        if suggestions:
            lines.append("### 💡 Suggested Next Steps:")
            for i, suggestion in enumerate(suggestions, 1):
                priority_icon = "🔴" if suggestion['priority'] == 'high' else "🟡"
                lines.append(f"{priority_icon} **{suggestion['command']}** - {suggestion['reason']}")

        lines.append("\n---")
        lines.append("💬 **Tip:** Just type what you want to do, and I'll help you find the right tool!")

        return "\n".join(lines)

    def get_tool_help(self, tool_name: str) -> Optional[str]:
        """
        Get detailed help for a specific tool.

        Args:
            tool_name: Name or command of the tool

        Returns:
            Help message or None if not found
        """
        tool_name_lower = tool_name.lower()

        for category in self.tool_categories.values():
            for tool in category['tools']:
                if (tool_name_lower in tool['name'].lower() or
                    tool_name_lower in tool['command'].lower()):

                    help_msg = f"## 📖 {tool['name']}\n\n"
                    help_msg += f"**Description:** {tool['description']}\n"
                    help_msg += f"**How to use:** Say `{tool['command']}`\n"

                    if tool['requires']:
                        help_msg += f"**Prerequisites:** "
                        prereqs = []
                        for req in tool['requires']:
                            if req == 'data_uploaded':
                                prereqs.append("Data must be uploaded")
                            elif req == 'analysis_complete':
                                prereqs.append("Analysis must be completed")
                            elif req == 'shapefile_uploaded':
                                prereqs.append("Shapefile must be uploaded")
                        help_msg += ", ".join(prereqs)

                    return help_msg

        return None

    def handle_discovery_request(self, request: str, session_data: Dict[str, Any]) -> str:
        """
        Handle various tool discovery requests.

        Args:
            request: User's request
            session_data: Current session data

        Returns:
            Appropriate response message
        """
        request_lower = request.lower()

        # Check for specific discovery patterns
        if any(phrase in request_lower for phrase in ['what can i do', 'available tools', 'show tools']):
            available = self.get_available_tools(session_data)
            suggestions = self.suggest_tools(session_data)
            return self.format_discovery_message(available, suggestions)

        if 'what now' in request_lower or 'what next' in request_lower:
            suggestions = self.suggest_tools(session_data)
            if suggestions:
                lines = ["### 💡 Here's what you can do next:\n"]
                for suggestion in suggestions[:3]:
                    lines.append(f"• **{suggestion['command']}** - {suggestion['reason']}")
                return "\n".join(lines)
            else:
                return "Start by uploading your data with the 📎 button!"

        if 'how to' in request_lower:
            # Extract tool name after "how to"
            parts = request_lower.split('how to')
            if len(parts) > 1:
                tool_query = parts[1].strip()
                help_msg = self.get_tool_help(tool_query)
                if help_msg:
                    return help_msg

        return "I can help you discover available tools. Ask 'what can I do?' or 'show available tools'."