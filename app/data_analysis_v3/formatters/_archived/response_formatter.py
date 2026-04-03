"""
Response Formatter for Data Analysis V3
Converts technical outputs to user-friendly insights
"""

import re
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def format_analysis_response(raw_output: str, state_updates: Dict[str, Any]) -> str:
    """
    Format raw Python output into user-friendly response.
    
    Args:
        raw_output: Raw stdout from Python execution
        state_updates: State updates including plots, errors, etc.
        
    Returns:
        Formatted user-friendly response
    """
    # If there are errors, return user-friendly error message
    if state_updates.get('errors'):
        return _format_error_response(state_updates['errors'])
    
    # Parse the raw output for insights
    insights = _extract_insights(raw_output)
    
    # Add visualization references if plots were generated
    if state_updates.get('output_plots'):
        num_plots = len(state_updates['output_plots'])
        if num_plots == 1:
            insights.append("I've created a visualization to help illustrate these findings.")
        else:
            insights.append(f"I've created {num_plots} visualizations to help illustrate these findings.")
    
    # Format the insights into natural language
    if insights:
        return _format_insights(insights)
    
    # If no specific insights, provide generic response
    return "I've completed the analysis. The results are shown above."


def _extract_insights(output: str) -> List[str]:
    """
    Extract meaningful insights from raw Python output.
    
    Args:
        output: Raw stdout from code execution
        
    Returns:
        List of insight strings
    """
    insights = []
    lines = output.strip().split('\n') if output else []
    
    # Check if this looks like a numbered list (for top N queries)
    has_numbered_list = any(re.match(r'^\s*\d+\.', line) for line in lines)
    
    if has_numbered_list:
        # This is a numbered list - preserve it entirely for health officials
        # Just clean up any technical artifacts but keep the structure
        for line in lines:
            if line.strip():
                # Don't modify numbered items or headers
                if re.match(r'^\s*\d+\.', line) or 'Top' in line or ':' in line:
                    insights.append(line)
                elif not line.startswith('<') and not line.startswith('['):
                    insights.append(line)
        return insights
    
    # Original logic for non-numbered outputs
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Convert DataFrame outputs to insights
        if 'DataFrame' in line or line.startswith('   '):
            continue  # Skip raw DataFrame representations
        
        # Look for statistical outputs
        if any(keyword in line.lower() for keyword in ['mean', 'average', 'total', 'count', 'sum']):
            insights.append(_format_statistical_line(line))
        
        # Look for comparison outputs
        elif any(keyword in line.lower() for keyword in ['highest', 'lowest', 'maximum', 'minimum']):
            insights.append(_format_comparison_line(line))
        
        # Look for percentage outputs
        elif '%' in line:
            insights.append(_format_percentage_line(line))
        
        # Generic insight
        elif line and not line.startswith('<') and not line.startswith('['):
            # Clean up technical notation
            cleaned = _clean_technical_notation(line)
            if cleaned:
                insights.append(cleaned)
    
    return insights


def _format_statistical_line(line: str) -> str:
    """Format a line containing statistical information."""
    # Remove technical prefixes
    line = re.sub(r'^\s*\d+\s+', '', line)  # Remove index numbers
    line = re.sub(r'dtype:\s*\w+', '', line)  # Remove dtype info
    
    # Make it more conversational
    if 'mean' in line.lower():
        line = line.replace('mean', 'average')
    
    return line.strip()


def _format_comparison_line(line: str) -> str:
    """Format a line containing comparison information."""
    # Remove technical notation
    line = re.sub(r'^\s*\d+\s+', '', line)
    
    # Add context if needed
    if line.startswith(('highest', 'lowest')):
        line = "The " + line
    
    return line.strip()


def _format_percentage_line(line: str) -> str:
    """Format a line containing percentage information."""
    # Round percentages to 1 decimal place
    line = re.sub(r'(\d+\.\d{2,})%', lambda m: f"{float(m.group(1)[:-1]):.1f}%", line)
    
    return line.strip()


def _clean_technical_notation(text: str) -> str:
    """Remove technical notation from text."""
    # Remove array notation
    text = re.sub(r'array\(\[.*?\]\)', '', text)
    
    # Remove pandas index notation
    text = re.sub(r'^\s*\d+\s+', '', text)
    
    # Remove dtype information
    text = re.sub(r'dtype:\s*\w+', '', text)
    
    # Remove Name: notation
    text = re.sub(r'Name:\s*\w+,?\s*', '', text)
    
    # Clean up extra whitespace
    text = ' '.join(text.split())
    
    return text.strip() if text and len(text) > 10 else ""


def _format_insights(insights: List[str]) -> str:
    """
    Format list of insights into natural language response.
    
    Args:
        insights: List of insight strings
        
    Returns:
        Formatted response
    """
    if not insights:
        return "Analysis complete."
    
    # Check if this is already a numbered list
    has_numbered_items = any(re.match(r'^\s*\d+\.', str(item)) for item in insights)
    
    if has_numbered_items:
        # This is already formatted as a numbered list - just return it as-is
        # This preserves the "Top 10 facilities" format for health officials
        return "\n".join(insights)
    
    if len(insights) == 1:
        return insights[0]
    
    # Format multiple insights
    response_parts = ["Based on the analysis:\n"]

    # CRITICAL FIX: Show ALL insights, not just 5
    # This was causing "top 10" queries to only show partial results
    for insight in insights:  # Removed [:5] limit
        if insight:
            response_parts.append(f"- {insight}\n")

    return "\n".join(response_parts)


def _format_error_response(errors: List[str]) -> str:
    """
    Format error messages for user consumption.
    
    Args:
        errors: List of error messages
        
    Returns:
        User-friendly error message
    """
    if not errors:
        return "I encountered an issue during analysis. Please try rephrasing your question."
    
    error = str(errors[0]).lower()
    
    # Map common errors to user-friendly messages
    if 'keyerror' in error or 'not in index' in error:
        return "I couldn't find that column in your data. Could you check the column name?"
    
    elif 'empty' in error or 'no data' in error:
        return "The data appears to be empty. Please make sure you've uploaded valid data."
    
    elif 'type' in error or 'could not convert' in error:
        return "There's a data type issue. The data might need different formatting for this analysis."
    
    else:
        return "I encountered an issue with that analysis. Let me try a different approach."


def format_visualization_reference(viz_type: str, viz_count: int = 1) -> str:
    """
    Generate natural language reference to visualizations.
    
    Args:
        viz_type: Type of visualization (map, chart, etc.)
        viz_count: Number of visualizations
        
    Returns:
        Natural language reference
    """
    if viz_count == 1:
        if viz_type == "map":
            return "As shown in the map above"
        elif viz_type == "chart":
            return "As illustrated in the chart"
        else:
            return "As visualized above"
    else:
        return f"As shown in the {viz_count} visualizations above"