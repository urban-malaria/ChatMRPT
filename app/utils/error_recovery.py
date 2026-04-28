"""
Error Recovery Helper Module

Provides helpful error messages with solutions and alternative approaches.
Helps users recover from errors without frustration.
"""

import logging
import re
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ErrorRecoveryHelper:
    """Helper class for error recovery and solution suggestions."""

    def __init__(self):
        """Initialize the error recovery helper."""
        # Define common error patterns and solutions
        self.error_patterns = {
            'file_format': {
                'patterns': [
                    r'unsupported.*file.*type',
                    r'invalid.*file.*format',
                    r'not.*csv.*excel',
                    r'expected.*csv'
                ],
                'solutions': [
                    "Ensure your file is in CSV or Excel format (.csv, .xlsx, .xls)",
                    "For shapefiles, upload as a ZIP file containing .shp, .shx, and .dbf files",
                    "Check that the file extension matches the actual format"
                ],
                'alternatives': [
                    "Try our sample data to see the expected format",
                    "Convert your file to CSV using Excel or Google Sheets"
                ]
            },
            'missing_column': {
                'patterns': [
                    r'missing.*column',
                    r'column.*not.*found',
                    r'keyerror.*wardname',
                    r'no.*column.*named'
                ],
                'solutions': [
                    "Check that your data has required columns: 'WardName' and 'population'",
                    "Column names are case-insensitive but spelling must match",
                    "Use 'describe my data' to see what columns you have"
                ],
                'alternatives': [
                    "Rename columns in Excel before uploading",
                    "Use our data requirements helper to validate your file first"
                ]
            },
            'ward_mismatch': {
                'patterns': [
                    r'ward.*not.*match',
                    r'ward.*name.*mismatch',
                    r'inconsistent.*ward',
                    r'unmatched.*wards'
                ],
                'solutions': [
                    "Ensure ward names are consistent between CSV and shapefile",
                    "Check for spelling differences or extra spaces",
                    "Ward names are case-sensitive for matching"
                ],
                'alternatives': [
                    "Export ward names from shapefile and use those in your CSV",
                    "Use fuzzy matching option if available"
                ]
            },
            'file_size': {
                'patterns': [
                    r'file.*too.*large',
                    r'exceeds.*maximum.*size',
                    r'file.*size.*limit',
                    r'payload.*too.*large'
                ],
                'solutions': [
                    "CSV/Excel files must be under 32MB",
                    "Shapefiles (ZIP) must be under 100MB",
                    "Remove unnecessary columns to reduce file size"
                ],
                'alternatives': [
                    "Split large datasets into smaller regions",
                    "Compress shapefiles more efficiently",
                    "Sample your data for initial testing"
                ]
            },
            'analysis_prerequisite': {
                'patterns': [
                    r'analysis.*not.*complete',
                    r'run.*analysis.*first',
                    r'no.*analysis.*results',
                    r'requires.*completed.*analysis'
                ],
                'solutions': [
                    "Complete the risk analysis before this step",
                    "Say 'analyze my data' to run the analysis",
                    "Check workflow progress to see what's pending"
                ],
                'alternatives': [
                    "Use 'show workflow progress' to see what steps are needed",
                    "Load sample data for a quick test run"
                ]
            },
            'data_not_loaded': {
                'patterns': [
                    r'no.*data.*available',
                    r'upload.*data.*first',
                    r'data.*not.*loaded',
                    r'no.*csv.*loaded'
                ],
                'solutions': [
                    "Upload your data using the 📎 button",
                    "Both CSV and shapefile are needed for full analysis",
                    "Check that upload was successful"
                ],
                'alternatives': [
                    "Try 'load sample data' to get started quickly",
                    "Use 'show data requirements' to see what's needed"
                ]
            },
            'numeric_conversion': {
                'patterns': [
                    r'could.*not.*convert.*numeric',
                    r'invalid.*literal.*for.*float',
                    r'non-numeric.*value',
                    r'type.*error.*numeric'
                ],
                'solutions': [
                    "Check that numeric columns don't contain text values",
                    "Remove or replace 'N/A', 'NULL', or other text in number columns",
                    "Ensure decimal points use '.' not ','"
                ],
                'alternatives': [
                    "Clean data in Excel before uploading",
                    "Use 0 or leave cells empty for missing values"
                ]
            },
            'memory_error': {
                'patterns': [
                    r'memory.*error',
                    r'out.*of.*memory',
                    r'cannot.*allocate.*memory'
                ],
                'solutions': [
                    "Reduce the size of your dataset",
                    "Close other browser tabs to free memory",
                    "Try processing fewer wards at once"
                ],
                'alternatives': [
                    "Process data in smaller batches",
                    "Remove unnecessary columns",
                    "Contact support for large dataset handling"
                ]
            }
        }

        # Define recovery suggestions by error type
        self.recovery_actions = {
            'upload': {
                'restart': "Clear session and start fresh upload",
                'validate': "Validate your data format first",
                'sample': "Try with sample data to test workflow"
            },
            'analysis': {
                'restart': "Clear results and re-run analysis",
                'parameters': "Adjust analysis parameters",
                'subset': "Try with subset of data first"
            },
            'visualization': {
                'refresh': "Refresh the page and try again",
                'different': "Try a different visualization type",
                'simpler': "Start with simpler visualizations"
            }
        }

    def analyze_error(self, error_message: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Analyze an error and provide recovery suggestions.

        Args:
            error_message: The error message to analyze
            context: Optional context about when error occurred

        Returns:
            Dictionary with error analysis and suggestions
        """
        error_lower = error_message.lower()
        matched_category = None
        confidence = 0

        # Find matching error pattern
        for category, pattern_info in self.error_patterns.items():
            for pattern in pattern_info['patterns']:
                if re.search(pattern, error_lower):
                    matched_category = category
                    confidence = 0.9
                    break
            if matched_category:
                break

        # If no exact match, try fuzzy matching
        if not matched_category:
            matched_category, confidence = self._fuzzy_match_error(error_lower)

        # Generate recovery plan
        if matched_category:
            pattern_info = self.error_patterns[matched_category]
            return {
                'error_type': matched_category,
                'confidence': confidence,
                'solutions': pattern_info['solutions'],
                'alternatives': pattern_info['alternatives'],
                'quick_actions': self._get_quick_actions(matched_category, context)
            }
        else:
            # Generic error handling
            return {
                'error_type': 'unknown',
                'confidence': 0,
                'solutions': [
                    "Check your input data format",
                    "Ensure all required fields are present",
                    "Try refreshing the page"
                ],
                'alternatives': [
                    "Contact support with the full error message",
                    "Try a different approach",
                    "Check the help documentation"
                ],
                'quick_actions': []
            }

    def _fuzzy_match_error(self, error_text: str) -> Tuple[Optional[str], float]:
        """Fuzzy match error to categories."""
        keyword_map = {
            'file_format': ['file', 'format', 'csv', 'excel', 'extension'],
            'missing_column': ['column', 'field', 'variable', 'missing'],
            'ward_mismatch': ['ward', 'match', 'shapefile', 'boundary'],
            'file_size': ['size', 'large', 'big', 'memory', 'limit'],
            'analysis_prerequisite': ['analysis', 'complete', 'first', 'required'],
            'data_not_loaded': ['data', 'upload', 'load', 'no data'],
            'numeric_conversion': ['numeric', 'number', 'float', 'convert'],
        }

        best_match = None
        best_score = 0

        for category, keywords in keyword_map.items():
            score = sum(1 for keyword in keywords if keyword in error_text)
            if score > best_score:
                best_score = score
                best_match = category

        confidence = min(best_score * 0.3, 0.7)  # Max 0.7 confidence for fuzzy match
        return best_match, confidence

    def _get_quick_actions(self, error_type: str, context: Dict[str, Any] = None) -> List[Dict]:
        """Get quick action buttons for error recovery."""
        actions = []

        if error_type == 'file_format':
            actions.extend([
                {'label': 'View Data Requirements', 'action': 'show_data_requirements'},
                {'label': 'Try Sample Data', 'action': 'load_sample_data'}
            ])
        elif error_type == 'missing_column':
            actions.extend([
                {'label': 'Check Required Columns', 'action': 'show_data_requirements'},
                {'label': 'Describe My Data', 'action': 'describe_data'}
            ])
        elif error_type == 'data_not_loaded':
            actions.extend([
                {'label': 'Upload Data', 'action': 'show_upload_dialog'},
                {'label': 'Load Sample Data', 'action': 'load_sample_data'}
            ])
        elif error_type == 'analysis_prerequisite':
            actions.extend([
                {'label': 'Run Analysis', 'action': 'run_analysis'},
                {'label': 'Check Progress', 'action': 'show_workflow_progress'}
            ])

        return actions

    def format_error_help(self, error_analysis: Dict[str, Any], error_message: str) -> str:
        """
        Format error help for display to user.

        Args:
            error_analysis: Analysis from analyze_error()
            error_message: Original error message

        Returns:
            Formatted help message
        """
        lines = []

        # Header
        lines.append("## ❌ Error Encountered\n")

        # Original error (truncated if too long)
        if len(error_message) > 200:
            lines.append(f"**Error:** {error_message[:200]}...\n")
        else:
            lines.append(f"**Error:** {error_message}\n")

        # Solutions
        if error_analysis['solutions']:
            lines.append("### 🔧 How to Fix:")
            for i, solution in enumerate(error_analysis['solutions'], 1):
                lines.append(f"{i}. {solution}")
            lines.append("")

        # Alternatives
        if error_analysis['alternatives']:
            lines.append("### 💡 Alternative Approaches:")
            for alternative in error_analysis['alternatives']:
                lines.append(f"• {alternative}")
            lines.append("")

        # Quick actions
        if error_analysis.get('quick_actions'):
            lines.append("### 🚀 Quick Actions:")
            for action in error_analysis['quick_actions']:
                lines.append(f"• **{action['label']}** - `{action['action']}`")
            lines.append("")

        # Confidence note
        if error_analysis['confidence'] < 0.5:
            lines.append("_Note: This is a general suggestion. The specific error might need different handling._")

        lines.append("\n---")
        lines.append("💬 Need more help? Just ask me for clarification or try a different approach!")

        return "\n".join(lines)

    def get_common_fixes(self) -> str:
        """
        Get a list of common fixes for frequent issues.

        Returns:
            Formatted message with common fixes
        """
        lines = []
        lines.append("## 🛠️ Common Issues & Quick Fixes\n")

        common_issues = [
            {
                'issue': "Upload fails",
                'fixes': [
                    "Check file is CSV or Excel format",
                    "Ensure file is under 32MB",
                    "Remove special characters from column names"
                ]
            },
            {
                'issue': "Analysis won't run",
                'fixes': [
                    "Ensure both CSV and shapefile are uploaded",
                    "Check that required columns exist",
                    "Verify ward names match between files"
                ]
            },
            {
                'issue': "Map not showing",
                'fixes': [
                    "Upload shapefile as ZIP",
                    "Ensure shapefile has .shp, .shx, .dbf files",
                    "Check ward name consistency"
                ]
            },
            {
                'issue': "ITN planning blocked",
                'fixes': [
                    "Complete risk analysis first",
                    "Ensure analysis generated rankings",
                    "Check that data has population column"
                ]
            }
        ]

        for issue_info in common_issues:
            lines.append(f"### ❓ {issue_info['issue']}:")
            for fix in issue_info['fixes']:
                lines.append(f"  ✓ {fix}")
            lines.append("")

        lines.append("---")
        lines.append("**Still stuck?** Describe your specific issue and I'll help!")

        return "\n".join(lines)

    def suggest_restart(self, context: str = None) -> str:
        """
        Suggest restart options when things are stuck.

        Args:
            context: Context about what user was trying to do

        Returns:
            Restart suggestions message
        """
        lines = []
        lines.append("## 🔄 Starting Fresh\n")

        if context == 'upload':
            lines.append("If uploads keep failing, try:")
            lines.append("1. **Clear and restart**: Refresh the page")
            lines.append("2. **Check your data**: Use our data validator first")
            lines.append("3. **Try sample data**: Test with our example files")

        elif context == 'analysis':
            lines.append("If analysis is stuck, try:")
            lines.append("1. **Re-upload data**: Sometimes a fresh start helps")
            lines.append("2. **Smaller dataset**: Test with fewer wards first")
            lines.append("3. **Check requirements**: Ensure all needed columns exist")

        else:
            lines.append("Sometimes starting over helps:")
            lines.append("1. **Refresh page**: Clear temporary issues")
            lines.append("2. **New session**: Start with fresh upload")
            lines.append("3. **Sample data**: Test the full workflow first")

        lines.append("\n💡 **Tip**: Save your work before restarting!")

        return "\n".join(lines)

    def create_error_report(self, errors: List[str]) -> Dict[str, Any]:
        """
        Create a summary report of multiple errors.

        Args:
            errors: List of error messages

        Returns:
            Error report with patterns and suggestions
        """
        report = {
            'total_errors': len(errors),
            'error_categories': {},
            'most_common': None,
            'suggested_fixes': []
        }

        # Categorize errors
        category_counts = {}
        for error in errors:
            analysis = self.analyze_error(error)
            error_type = analysis['error_type']

            if error_type not in category_counts:
                category_counts[error_type] = 0
            category_counts[error_type] += 1

            if error_type not in report['error_categories']:
                report['error_categories'][error_type] = {
                    'count': 0,
                    'solutions': analysis['solutions']
                }
            report['error_categories'][error_type]['count'] += 1

        # Find most common error
        if category_counts:
            report['most_common'] = max(category_counts, key=category_counts.get)

            # Get primary fixes for most common error
            if report['most_common'] in self.error_patterns:
                report['suggested_fixes'] = self.error_patterns[report['most_common']]['solutions']

        return report