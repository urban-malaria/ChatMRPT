"""
Message Formatters

Handles formatting of messages for various stages of the data analysis workflow.
Includes ResponseFormatter for data output formatting (DataFrames, lists, statistics).
"""

import logging
import pandas as pd
import re
from typing import Dict, Any, List, Optional, Union
from pathlib import Path

logger = logging.getLogger(__name__)


class MessageFormatter:
    """Formats messages for different workflow stages."""
    
    def __init__(self, session_id: str):
        """
        Initialize the message formatter.
        
        Args:
            session_id: Session identifier
        """
        self.session_id = session_id
    
    def format_state_selection(self, analysis: Dict) -> str:
        """Format state selection message with statistics."""
        message = "Great! I'll guide you through the TPR calculation process.\n\n"
        
        # Check if there's only one state (should not happen since we skip this)
        if analysis.get('total_states') == 1:
            # This shouldn't be called if single state, but just in case
            single_state = list(analysis['states'].keys())[0]
            message += f"I see you have data for **{single_state}**.\n\n"
            return message
        
        message += "**Which state would you like to analyze?**\n\n"
        
        if 'states' in analysis and analysis['states']:
            message += "Based on your data, I found:\n\n"
            for idx, (state, info) in enumerate(analysis['states'].items(), 1):
                message += f"**{idx}. {info['name']}**\n"
                message += f"   - {info['total_records']:,} records\n"
                message += f"   - {info['facilities']:,} facilities\n"
                message += f"   - {info['total_tests']:,} total tests\n\n"
            
            message += "Which state would you like to analyze? (Enter the number or state name)"
            
            # Add recommendation if available
            if analysis.get('recommended'):
                message += f"\n\nNote: {analysis['recommended']} has the most complete data."
        
        return message
    
    def format_facility_selection(self, state: str, analysis: Dict) -> str:
        """Format facility level selection message conversationally."""
        message = f"Now, which health facility level would you like to analyze?\n\n"

        if 'levels' in analysis and analysis['levels']:
            levels = analysis['levels']

            # Show statistics for each level (excluding 'all' for now)
            for level_key, level_data in levels.items():
                if level_key == 'all':
                    continue  # Skip 'all', we'll add it at the end

                # Get statistics
                level_name = level_data.get('name', level_key)
                facility_count = level_data.get('count', 0)
                rdt_tests = level_data.get('rdt_tests', 0)
                microscopy_tests = level_data.get('microscopy_tests', 0)
                total_tests = rdt_tests + microscopy_tests

                # Mark recommended
                is_recommended = level_data.get('recommended', False)
                rec = " **(Recommended)**" if is_recommended else ""

                message += f"- **{level_key}**{rec} - {level_name}\n"
                message += f"  - {facility_count} facilities, {total_tests:,} tests\n\n"

            # Add 'all' option at the end
            if 'all' in levels:
                all_data = levels['all']
                all_count = all_data.get('count', 0)
                message += f"\n- **all** - All facility levels combined\n"
                message += f"  - {all_count} facilities\n\n"

            message += "**Need help deciding?**\n"
            message += "- Ask me to **'show facility charts'** or **'explain the differences'**\n\n"
            message += "Just type which level you'd like: **primary**, **secondary**, **tertiary**, or **all**"
        else:
            message += "I'll analyze all facilities in your dataset."

        return message

    def format_facility_selection_only(self, analysis: Dict) -> str:
        """Format facility selection when state is auto-selected."""
        message = "Now, which health facility level would you like to analyze?\n\n"

        if 'levels' in analysis and analysis['levels']:
            levels = analysis['levels']

            # Show statistics for each level (excluding 'all' for now)
            for level_key, level_data in levels.items():
                if level_key == 'all':
                    continue  # Skip 'all', we'll add it at the end

                # Get statistics
                level_name = level_data.get('name', level_key)
                facility_count = level_data.get('count', 0)
                rdt_tests = level_data.get('rdt_tests', 0)
                microscopy_tests = level_data.get('microscopy_tests', 0)
                total_tests = rdt_tests + microscopy_tests

                # Mark recommended
                is_recommended = level_data.get('recommended', False)
                rec = " **(Recommended)**" if is_recommended else ""

                message += f"- **{level_key}**{rec} - {level_name}\n"
                message += f"  - {facility_count} facilities, {total_tests:,} tests\n\n"

            # Add 'all' option at the end
            if 'all' in levels:
                all_data = levels['all']
                all_count = all_data.get('count', 0)
                message += f"\n- **all** - All facility levels combined\n"
                message += f"  - {all_count} facilities\n\n"

            message += "**Need help deciding?**\n"
            message += "- Ask me to **'show facility charts'** or **'explain the differences'**\n\n"
            message += "Just type which level you'd like: **primary**, **secondary**, **tertiary**, or **all**"
        else:
            message += "I'll analyze all facilities in your dataset."

        return message

    def format_age_group_selection(self, analysis: Dict) -> str:
        """Format age group selection message with statistics and recommendation."""
        # Get state and facility from the analysis dict
        state = analysis.get('state', 'the selected state')

        # Format facility level properly
        facility_raw = analysis.get('facility_level', 'selected facilities')
        facility_map = {
            'primary': 'Primary facilities',
            'secondary': 'Secondary facilities',
            'tertiary': 'Tertiary facilities',
            'all': 'all facilities'
        }
        facility = facility_map.get(facility_raw, facility_raw)

        message = f"Perfect! **{facility}** selected.\n\n"
        message += "## Step 2: Choose Age Group\n\n"
        message += "Select which age group to analyze. Here's what your data contains:\n\n"

        if 'age_groups' in analysis:
            age_groups = analysis['age_groups']

            # Add statistics for each age group
            for group_key, group_data in age_groups.items():
                group_names = {
                    'u5': 'Under 5 years',
                    'o5': 'Over 5 years',
                    'pw': 'Pregnant Women'
                }
                group_name = group_names.get(group_key, group_key)

                tests = group_data.get('total_tests', 0)
                positive = group_data.get('total_positive', 0)
                tpr = (positive / tests * 100) if tests > 0 else 0

                # Add recommendation for u5
                rec = " **(Recommended)**" if group_key == 'u5' else ""

                message += f"- **{group_key}**{rec} - {group_name}\n"
                message += f"  - {tests:,} tests, TPR: {tpr:.1f}%\n\n"

            message += f"- **all** - All age groups combined\n"
            total_tests = analysis.get('total_tests', 0)
            message += f"  - {total_tests:,} total tests\n\n"

            message += "**Need help deciding?**\n"
            message += "- Ask me to **'show age charts'** or **'explain the differences'**\n\n"
            message += "Just type which group you'd like: **u5**, **o5**, **pw**, or **all**"
        else:
            message += "All age groups will be analyzed.\n"

        return message
    
    def format_tool_tpr_results(self, tool_output: str) -> str:
        """Format Malaria Burden tool results for display."""
        # The tool already returns well-formatted text, just pass it through with minor enhancements
        if "Malaria Burden Analysis Complete" in tool_output or "TPR Analysis Complete" in tool_output:
            # The tool output is already well formatted, just use it
            message = tool_output

            # ✅ REMOVED map check - visualization objects are returned separately, not in message text
            # The workflow_manager now handles visualization objects in the visualizations array
            # No need to check for iframe or map file mentions in the text

            # ✅ REMOVED CONFIRMATION PROMPT - Auto-transition handles this now!
            # The workflow now automatically transitions to standard mode after TPR completes
            # No need to wait for user confirmation

            # Set flag that TPR is complete and ready for risk analysis
            import os
            flag_file = f"instance/uploads/{self.session_id}/.tpr_complete"
            Path(flag_file).touch()

            return message
        else:
            # Fallback formatting if tool output is unexpected
            return tool_output


class ResponseFormatter:
    """Unified response formatter for all ChatMRPT data outputs"""

    @staticmethod
    def format_dataframe(
        df: pd.DataFrame,
        limit: int = 10,
        show_index: bool = False,
        title: Optional[str] = None
    ) -> str:
        """Convert DataFrame to markdown table with optional limit"""
        logger.info(f"[DEBUG FORMATTER] format_dataframe called with {len(df) if df is not None else 0} rows, limit={limit}")

        if df is None or len(df) == 0:
            logger.info(f"[DEBUG FORMATTER] DataFrame is empty, returning early")
            return "*No data to display*"

        # Build header
        row_count = len(df)
        logger.info(f"[DEBUG FORMATTER] DataFrame has {row_count} rows, {len(df.columns)} columns")

        if title:
            header = f"**{title}**\n\n"
        else:
            header = ""

        # Handle large results
        if row_count > limit:
            logger.info(f"[DEBUG FORMATTER] Large result ({row_count} rows), showing first {limit}")
            header += f"*Showing first {limit} of {row_count:,} rows:*\n\n"
            df_display = df.head(limit)
        else:
            logger.info(f"[DEBUG FORMATTER] Small result ({row_count} rows), showing all")
            header += f"*{row_count:,} row{'s' if row_count != 1 else ''}:*\n\n"
            df_display = df

        # Convert to markdown table
        try:
            logger.info(f"[DEBUG FORMATTER] Converting to markdown table...")
            md_table = df_display.to_markdown(index=show_index, tablefmt='github')
            logger.info(f"[DEBUG FORMATTER] Markdown conversion successful, length={len(md_table)}")
            return f"{header}{md_table}"
        except Exception as e:
            # Fallback to string representation
            logger.warning(f"[DEBUG FORMATTER] Markdown table conversion failed: {e}. Using string fallback.")
            return f"{header}```\n{df_display.to_string(index=show_index)}\n```"

    @staticmethod
    def format_list(
        items: List[Any],
        style: str = 'bullet',
        max_items: Optional[int] = None,
        title: Optional[str] = None
    ) -> str:
        """Format list with proper line breaks"""
        if not items:
            return "*No items to display*"

        # Build header
        header = f"**{title}**\n\n" if title else ""

        # Handle large lists
        if max_items and len(items) > max_items:
            items_display = items[:max_items]
            suffix = f"\n\n*...and {len(items) - max_items:,} more*"
        else:
            items_display = items
            suffix = ""

        # Format based on style
        if style == 'bullet':
            formatted = '\n'.join([f"- {item}" for item in items_display])
        elif style == 'numbered':
            formatted = '\n'.join([f"{i+1}. {item}" for i, item in enumerate(items_display)])
        else:  # plain
            formatted = '\n'.join([str(item) for item in items_display])

        return f"{header}{formatted}{suffix}"

    @staticmethod
    def format_statistics(
        stats_dict: Dict[str, Any],
        title: str = "Statistics"
    ) -> str:
        """Format statistics dictionary with bullets and proper formatting"""
        if not stats_dict:
            return "*No statistics available*"

        lines = [f"**{title}:**\n"]

        for key, value in stats_dict.items():
            # Format based on value type
            if isinstance(value, float):
                formatted_value = f"{value:.2f}"
            elif isinstance(value, int):
                formatted_value = f"{value:,}"
            else:
                formatted_value = str(value)

            lines.append(f"- **{key}**: {formatted_value}")

        return '\n'.join(lines)

    @staticmethod
    def format_correlation(
        corr_value: float,
        var1: str,
        var2: str,
        include_interpretation: bool = True
    ) -> str:
        """Format correlation with interpretation"""
        # Interpret strength
        abs_corr = abs(corr_value)
        if abs_corr > 0.7:
            strength = "strong"
        elif abs_corr > 0.4:
            strength = "moderate"
        elif abs_corr > 0.2:
            strength = "weak"
        else:
            strength = "very weak"

        # Interpret direction
        direction = "positive" if corr_value > 0 else "negative"

        result = f"""**Correlation between {var1} and {var2}:**

- Coefficient: **{corr_value:.3f}**
- Strength: {strength.capitalize()}
- Direction: {direction.capitalize()}"""

        if include_interpretation:
            result += f"\n\n*There is a {strength} {direction} relationship between {var1} and {var2}.*"

        return result

    @staticmethod
    def normalize_spacing(text: str) -> str:
        """Normalize vertical spacing in text"""
        if not text:
            return ""

        # Normalize line endings
        text = text.replace('\r\n', '\n').replace('\r', '\n')

        # Normalize bullet points to a consistent style without rewriting numbered lists
        text = re.sub(r'^\*\s+', '- ', text, flags=re.MULTILINE)
        text = re.sub(r'^•\s*', '- ', text, flags=re.MULTILINE)

        # Collapse 3+ blank lines to 2 blank lines
        text = re.sub(r'\n{3,}', '\n\n', text)

        # Ensure text ends with single newline
        text = text.rstrip('\n') + '\n'

        return text

    @staticmethod
    def format_query_result(
        data: Union[pd.DataFrame, List, Dict, Any],
        query_description: Optional[str] = None
    ) -> str:
        """Auto-detect data type and format appropriately"""
        header = f"**{query_description}**\n\n" if query_description else ""

        if isinstance(data, pd.DataFrame):
            return header + ResponseFormatter.format_dataframe(data)
        elif isinstance(data, list):
            return header + ResponseFormatter.format_list(data)
        elif isinstance(data, dict):
            return header + ResponseFormatter.format_statistics(data)
        elif isinstance(data, (int, float)):
            formatted_value = f"{data:,.2f}" if isinstance(data, float) else f"{data:,}"
            return f"{header}**Result:** {formatted_value}"
        else:
            return f"{header}{str(data)}"


# Convenience functions for quick formatting
def format_df(df: pd.DataFrame, limit: int = 10) -> str:
    """Quick DataFrame formatter"""
    return ResponseFormatter.format_dataframe(df, limit=limit)


def format_list(items: List[Any], max_items: int = None) -> str:
    """Quick list formatter"""
    return ResponseFormatter.format_list(items, max_items=max_items)


def format_stats(stats: Dict[str, Any]) -> str:
    """Quick statistics formatter"""
    return ResponseFormatter.format_statistics(stats)
