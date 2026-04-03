"""
Standard Mode Tools — Bridge Layer for V3 Agent

Thin @tool wrappers that let the LangGraph agent call existing standard
mode tools. The existing tool code is NOT modified — these wrappers
just adapt the interface for LangGraph.

Pattern: each wrapper
  1. Receives graph_state (InjectedState) for session_id + output_plots
  2. Creates the existing tool instance with user parameters
  3. Calls .execute(session_id)
  4. Adds any HTML/viz output to graph_state['output_plots']
  5. Returns (message, {}) tuple for LangGraph
"""

import os
import logging
from typing import Tuple, Dict, Any, Annotated, Optional, List
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState

logger = logging.getLogger(__name__)


def _add_viz_to_state(graph_state: dict, file_path: str) -> None:
    """Add a visualization file path to graph_state['output_plots']."""
    if file_path and os.path.exists(file_path):
        if 'output_plots' not in graph_state:
            graph_state['output_plots'] = []
        graph_state['output_plots'].append(file_path)
        logger.info(f"Added viz to graph state: {file_path}")


# ─────────────────────────────────────────────────────────────────────
# 1. VARIABLE DISTRIBUTION MAP
# ─────────────────────────────────────────────────────────────────────

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
        variable_name: The column name to visualize. Fuzzy-matched.
        geographic_level: 'ward' (default) or 'lga' (aggregated).
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        from app.tools.variable_distribution import VariableDistribution
        result = VariableDistribution(
            variable_name=variable_name,
            geographic_level=geographic_level,
        ).execute(session_id=session_id)

        if not result.success:
            return result.message, {}

        data = result.data or {}
        _add_viz_to_state(graph_state, data.get('file_path'))
        return result.message or f"Created distribution map for {variable_name}.", {}
    except Exception as e:
        logger.error(f"create_variable_map failed: {e}", exc_info=True)
        return f"Error creating map: {e}", {}


# ─────────────────────────────────────────────────────────────────────
# 2. VULNERABILITY / RISK MAP (composite + PCA)
# ─────────────────────────────────────────────────────────────────────

@tool
def create_vulnerability_map(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    method: str = "composite",
) -> Tuple[str, Dict[str, Any]]:
    """Create a vulnerability/risk classification map showing ward risk levels.

    Use this AFTER risk analysis is complete. Shows wards colored by their
    risk category (High/Medium/Low) based on composite or PCA scores.

    Args:
        thought: Your reasoning about which method to show and why.
        method: 'composite' (default) or 'pca'. Use 'composite' for the
                weighted variable approach, 'pca' for principal component approach.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        if method.lower() == 'pca':
            from app.tools.visualization_maps_tools import CreatePCAMap
            result = CreatePCAMap().execute(session_id=session_id)
        else:
            from app.tools.visualization_maps_tools import CreateVulnerabilityMap
            result = CreateVulnerabilityMap().execute(session_id=session_id)

        if not result.success:
            return result.message, {}

        data = result.data or {}
        _add_viz_to_state(graph_state, data.get('file_path'))
        return result.message or f"Created {method} vulnerability map.", {}
    except Exception as e:
        logger.error(f"create_vulnerability_map failed: {e}", exc_info=True)
        return f"Error creating vulnerability map: {e}", {}


# ─────────────────────────────────────────────────────────────────────
# 3. COMPOSITE SCORE MAPS (multi-subplot model breakdown)
# ─────────────────────────────────────────────────────────────────────

@tool
def create_composite_score_maps(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    page: int = 1,
) -> Tuple[str, Dict[str, Any]]:
    """Create paginated maps showing individual risk model breakdowns.

    Use this AFTER risk analysis to show how each variable combination
    (model) scores wards differently. Shows 4 models per page.

    Args:
        thought: Your reasoning about showing model breakdowns.
        page: Page number (default 1). Each page shows up to 4 models.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        from app.tools.visualization_maps_tools import CreateCompositeScoreMaps
        result = CreateCompositeScoreMaps(
            models_per_page=4,
            page=page,
        ).execute(session_id=session_id)

        if not result.success:
            return result.message, {}

        data = result.data or {}
        _add_viz_to_state(graph_state, data.get('file_path'))
        return result.message or "Created composite score breakdown maps.", {}
    except Exception as e:
        logger.error(f"create_composite_score_maps failed: {e}", exc_info=True)
        return f"Error creating composite maps: {e}", {}


# ─────────────────────────────────────────────────────────────────────
# 4. URBAN EXTENT MAP
# ─────────────────────────────────────────────────────────────────────

@tool
def create_urban_extent_map(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    threshold: float = 50.0,
) -> Tuple[str, Dict[str, Any]]:
    """Create a map showing urban vs rural areas with risk overlay.

    Wards below the urban threshold are greyed out. Wards above are
    color-coded by vulnerability. Helps identify rural high-risk areas.

    Args:
        thought: Your reasoning about urban/rural analysis.
        threshold: Urban percentage cutoff (0-100). Default 50%.
                   Wards below this % are considered rural and greyed out.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        from app.tools.visualization_maps_tools import CreateUrbanExtentMap
        result = CreateUrbanExtentMap(
            threshold=threshold,
        ).execute(session_id=session_id)

        if not result.success:
            return result.message, {}

        data = result.data or {}
        _add_viz_to_state(graph_state, data.get('file_path'))
        return result.message or f"Created urban extent map (threshold {threshold}%).", {}
    except Exception as e:
        logger.error(f"create_urban_extent_map failed: {e}", exc_info=True)
        return f"Error creating urban extent map: {e}", {}


# ─────────────────────────────────────────────────────────────────────
# 5. MALARIA RISK ANALYSIS (composite + PCA)
# ─────────────────────────────────────────────────────────────────────

@tool
def run_risk_analysis(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
) -> Tuple[str, Dict[str, Any]]:
    """Run comprehensive malaria risk analysis using dual methods.

    Performs BOTH Composite Scoring and PCA analysis on the uploaded data.
    Creates unified_dataset.csv with ward-level risk rankings.
    Must be run BEFORE vulnerability maps or ITN planning.

    This is a heavy operation — auto-selects region-specific environmental
    variables, normalizes data, runs spatial imputation, computes scores,
    and creates the unified dataset.

    Args:
        thought: Your reasoning about running risk analysis.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        from app.tools.complete_analysis_tools import RunMalariaRiskAnalysis
        result = RunMalariaRiskAnalysis().execute(session_id=session_id)

        if not result.success:
            return result.message, {}

        return result.message or "Risk analysis complete. Unified dataset created.", {}
    except Exception as e:
        logger.error(f"run_risk_analysis failed: {e}", exc_info=True)
        return f"Error running risk analysis: {e}", {}


# ─────────────────────────────────────────────────────────────────────
# 6. ITN DISTRIBUTION PLANNING
# ─────────────────────────────────────────────────────────────────────

@tool
def plan_itn_distribution(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    total_nets: int,
    avg_household_size: float = 5.0,
    urban_threshold: float = 75.0,
    method: str = "composite",
) -> Tuple[str, Dict[str, Any]]:
    """Plan optimal ITN (bed net) distribution across wards.

    Allocates nets using a two-tier system: rural high-risk wards first,
    then urban wards with remaining nets. Produces a map, CSV export,
    and interactive dashboard.

    Must be run AFTER risk analysis is complete.

    Args:
        thought: Your reasoning about ITN planning parameters.
        total_nets: Total number of nets available for distribution.
        avg_household_size: Average household size (default 5.0).
        urban_threshold: Urban % cutoff for rural priority (default 75%).
        method: 'composite' (default) or 'pca' ranking method.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        from app.tools.itn_planning_tools import PlanITNDistribution
        result = PlanITNDistribution(
            total_nets=total_nets,
            avg_household_size=avg_household_size,
            urban_threshold=urban_threshold,
            method=method,
        ).execute(session_id=session_id)

        if not result.success:
            return result.message, {}

        data = result.data or {}
        # ITN produces a map
        _add_viz_to_state(graph_state, data.get('file_path'))

        # Include download links in the message
        message = result.message or "ITN distribution plan created."
        download_links = data.get('download_links', [])
        if download_links:
            message += "\n\n**Downloads available:**"
            for link in download_links:
                message += f"\n- [{link.get('description', 'Download')}]({link.get('url', '')})"

        return message, {}
    except Exception as e:
        logger.error(f"plan_itn_distribution failed: {e}", exc_info=True)
        return f"Error planning ITN distribution: {e}", {}


# ─────────────────────────────────────────────────────────────────────
# 7. SWITCH TPR COMBINATION
# ─────────────────────────────────────────────────────────────────────

@tool
def switch_tpr_combination(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    facility_level: str = "all",
    age_group: str = "all_ages",
) -> Tuple[str, Dict[str, Any]]:
    """Switch to a different TPR facility/age group combination.

    Regenerates raw_data.csv and shapefile for the new combination using
    cached ward data. Enables running risk analysis and ITN planning on
    different subsets without re-uploading.

    Args:
        thought: Your reasoning about switching combinations.
        facility_level: 'primary', 'secondary', 'tertiary', or 'all'.
        age_group: 'u5' (under 5), 'o5' (over 5), 'pw' (pregnant women),
                   or 'all_ages'.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        from app.tools.tpr_query_tool import SwitchTPRCombination
        result = SwitchTPRCombination(
            facility_level=facility_level,
            age_group=age_group,
        ).execute(session_id=session_id)

        if not result.success:
            return result.message, {}

        data = result.data or {}
        _add_viz_to_state(graph_state, data.get('file_path'))
        return result.message or f"Switched to {facility_level}/{age_group}.", {}
    except Exception as e:
        logger.error(f"switch_tpr_combination failed: {e}", exc_info=True)
        return f"Error switching combination: {e}", {}
