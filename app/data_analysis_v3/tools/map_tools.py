"""
Map Tools for V3 Agent — Bridge Layer

Thin wrappers that let the LangGraph agent call existing standard mode
visualization tools. The existing tool code is NOT modified — these
wrappers just adapt the interface.
"""

import os
import logging
from typing import Tuple, Dict, Any, Annotated, Optional
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState

logger = logging.getLogger(__name__)


@tool
def create_variable_map(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    variable_name: str,
    geographic_level: str = "ward",
) -> Tuple[str, Dict[str, Any]]:
    """Create a spatial distribution map for any variable in the dataset.

    Use this when users ask to map, plot, or visualize how a variable
    (like TPR, rainfall, elevation, housing_quality, Burden, etc.)
    varies across wards or LGAs.

    Args:
        thought: Your reasoning about what variable to map and why.
        variable_name: The column name to visualize (e.g., 'Burden', 'TPR',
                       'rainfall'). Fuzzy-matched — close names will work.
        geographic_level: 'ward' (default, shows all wards) or 'lga'
                         (aggregated to LGA level).
    """
    session_id = graph_state.get('session_id', 'default')

    try:
        from app.tools.variable_distribution import VariableDistribution

        # Create the tool instance with the user's parameters
        tool_instance = VariableDistribution(
            variable_name=variable_name,
            geographic_level=geographic_level,
        )

        # Execute — the existing tool handles everything:
        # data loading, shapefile merge, fuzzy matching, map creation
        result = tool_instance.execute(session_id=session_id)

        if not result.success:
            return result.message, {}

        # Extract the visualization path
        data = result.data or {}
        file_path = data.get('file_path')
        web_path = data.get('web_path', '')

        # CRITICAL: Directly mutate graph_state to add output_plots,
        # same pattern as analyze_data tool. This is how the agent's
        # _process_visualizations picks them up after graph completion.
        if file_path and os.path.exists(file_path):
            if 'output_plots' not in graph_state:
                graph_state['output_plots'] = []
            graph_state['output_plots'].append(file_path)
            logger.info(f"Added map to graph state output_plots: {file_path}")

        # Build a rich text response with the map context
        message = result.message or f"Created spatial distribution map for {variable_name}."

        # Include the web path so the response can reference it
        if web_path:
            message += f"\n\n[Map available at: {web_path}]"

        return message, {}

    except Exception as e:
        logger.error(f"create_variable_map failed: {e}", exc_info=True)
        return f"Error creating map: {str(e)}", {}
