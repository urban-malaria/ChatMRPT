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

import json
import os
import logging
from typing import Tuple, Dict, Any, Annotated, Optional, List
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState

logger = logging.getLogger(__name__)


def _safe_dict(value: Any) -> Dict[str, Any]:
    """Return dict values only; ToolExecutionResult extras may be pydantic models."""
    return value if isinstance(value, dict) else {}


def _add_canonical_response(
    graph_state: dict,
    *,
    tool_name: str,
    message: str,
    success: bool,
    metadata: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    priority: int = 100,
    source: str = "tool_result",
) -> None:
    """Capture deterministic tool output for final user display."""
    message = (message or "").strip()
    if not message:
        return

    metadata = metadata or {}
    data = data or {}
    requires_user_input = bool(
        metadata.get("requires_user_input")
        or data.get("waiting_for_parameters")
        or data.get("requires_user_input")
    )
    if requires_user_input:
        priority = max(priority, 120)
    elif not success:
        priority = max(priority, 110)

    responses = graph_state.setdefault("canonical_responses", [])
    responses.append({
        "tool_name": tool_name,
        "message": message,
        "success": bool(success),
        "requires_user_input": requires_user_input,
        "priority": priority,
        "source": source,
        "metadata": metadata,
        "sequence": len(responses) + 1,
    })
    logger.info(
        "Captured canonical response: tool=%s success=%s requires_input=%s priority=%s",
        tool_name,
        success,
        requires_user_input,
        priority,
    )


def _add_viz_to_state(graph_state: dict, file_path: str) -> None:
    """Add a visualization file path to graph_state['output_plots']."""
    if file_path and os.path.exists(file_path):
        if 'output_plots' not in graph_state:
            graph_state['output_plots'] = []
        graph_state['output_plots'].append(file_path)
        logger.info(f"Added viz to graph state: {file_path}")


def _resolve_year_tag(session_id: str, year: Optional[int],
                      required_file: str = 'unified_dataset') -> Tuple[str, Optional[str]]:
    """Return (year_tag, error_message). error_message is None when year data is ready."""
    if year is None:
        return '', None
    year_tag = f'_{year}'
    session_folder = os.path.join('instance', 'uploads', session_id)
    # Check .geoparquet first (per-year unified datasets are saved as geoparquet)
    geoparquet_path = os.path.join(session_folder, f'{required_file}{year_tag}.geoparquet')
    csv_path = os.path.join(session_folder, f'{required_file}{year_tag}.csv')
    if not (os.path.exists(geoparquet_path) or os.path.exists(csv_path)):
        bg_status = 'computing'
        status_path = os.path.join(session_folder, 'multi_year_vuln_status.json')
        if os.path.exists(status_path):
            with open(status_path) as f:
                s = json.load(f)
            completed = s.get('completed_years', [])
            bg_status = 'complete' if year_tag in completed else 'still computing'
        return year_tag, (
            f"Data for {year} is {bg_status}. Please try again in a moment."
        )
    return year_tag, None


# ─────────────────────────────────────────────────────────────────────
# 1. VARIABLE DISTRIBUTION MAP
# ─────────────────────────────────────────────────────────────────────

@tool
def create_variable_map(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    variable_name: str,
    geographic_level: str = "ward",
    year: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Create a spatial distribution map for any variable in the dataset.

    Use this when users ask to map, plot, or visualize how a variable
    (like TPR, rainfall, elevation, housing_quality, Burden, etc.)
    varies across wards or LGAs.

    Args:
        thought: Your reasoning about what variable to map and why.
        variable_name: The column name to visualize. Fuzzy-matched.
        geographic_level: 'ward' (default) or 'lga' (aggregated).
        year: Optional year (e.g. 2023) for multi-year datasets. None uses
              the aggregate (single-year or merged) data.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        year_tag, err = _resolve_year_tag(session_id, year, required_file='raw_data')
        if err:
            return err, {}

        from app.visualization.variable_distribution import VariableDistribution
        result = VariableDistribution(
            variable_name=variable_name,
            geographic_level=geographic_level,
            year_tag=year_tag,
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
    year: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Create a vulnerability/risk classification map showing ward risk levels.

    Use this AFTER risk analysis is complete. Shows wards colored by their
    risk category (High/Medium/Low) based on composite or PCA scores.

    Args:
        thought: Your reasoning about which method to show and why.
        method: 'composite' (default) or 'pca'. Use 'composite' for the
                weighted variable approach, 'pca' for principal component approach.
        year: Optional year (e.g. 2022) to map year-specific risk results.
              None uses the aggregate unified dataset.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        year_tag, err = _resolve_year_tag(session_id, year)
        if err:
            return err, {}

        session_folder = os.path.join('instance', 'uploads', session_id)

        # Detect multi-year mode: per-year unified datasets present + no specific year requested
        import glob as _glob
        year_geoparquets = _glob.glob(os.path.join(session_folder, 'unified_dataset_*.geoparquet'))
        use_multi_year = bool(year_geoparquets) and year is None

        if use_multi_year:
            if method.lower() == 'pca':
                from app.visualization.maps_tools import CreateMultiYearPCAMap
                result = CreateMultiYearPCAMap().execute(session_id=session_id)
            else:
                from app.visualization.maps_tools import CreateMultiYearVulnerabilityMap
                result = CreateMultiYearVulnerabilityMap().execute(session_id=session_id)

                # Also build PCA multi-year map if PCA data available
                pca_status_path = os.path.join(session_folder, 'multi_year_vuln_status.json')
                try:
                    from app.visualization.maps_tools import CreateMultiYearPCAMap
                    pca_result = CreateMultiYearPCAMap().execute(session_id=session_id)
                    if pca_result.success and pca_result.data:
                        _add_viz_to_state(graph_state, pca_result.data.get('file_path'))
                except Exception as pca_err:
                    logger.info(f"PCA multi-year map skipped: {pca_err}")
        elif method.lower() == 'pca':
            from app.visualization.maps_tools import CreatePCAMap
            result = CreatePCAMap(year_tag=year_tag).execute(session_id=session_id)
        else:
            from app.visualization.maps_tools import CreateVulnerabilityMap
            result = CreateVulnerabilityMap(year_tag=year_tag).execute(session_id=session_id)

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
    year: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Create paginated maps showing individual risk model breakdowns.

    Use this AFTER risk analysis to show how each variable combination
    (model) scores wards differently. Shows 4 models per page.

    Args:
        thought: Your reasoning about showing model breakdowns.
        page: Page number (default 1). Each page shows up to 4 models.
        year: Optional year for year-specific model breakdown maps.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        year_tag, err = _resolve_year_tag(session_id, year)
        if err:
            return err, {}

        from app.visualization.maps_tools import CreateCompositeScoreMaps
        result = CreateCompositeScoreMaps(
            models_per_page=4,
            page=page,
            year_tag=year_tag,
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
    year: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Create a map showing urban vs rural areas with risk overlay.

    Wards below the urban threshold are greyed out. Wards above are
    color-coded by vulnerability. Helps identify rural high-risk areas.

    Args:
        thought: Your reasoning about urban/rural analysis.
        threshold: Urban percentage cutoff (0-100). Default 50%.
                   Wards below this % are considered rural and greyed out.
        year: Optional year for year-specific urban extent maps.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        year_tag, err = _resolve_year_tag(session_id, year)
        if err:
            return err, {}

        from app.visualization.maps_tools import CreateUrbanExtentMap
        result = CreateUrbanExtentMap(
            threshold=threshold,
            year_tag=year_tag,
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
        from app.analysis.complete_tools import RunMalariaRiskAnalysis
        result = RunMalariaRiskAnalysis().execute(session_id=session_id)
        message = result.message or "Risk analysis complete. Unified dataset created."
        _add_canonical_response(
            graph_state,
            tool_name="run_risk_analysis",
            message=message,
            success=result.success,
            metadata=_safe_dict(result.metadata),
            data=_safe_dict(result.data),
            priority=100,
        )

        if not result.success:
            return message, {}

        return message, {}
    except Exception as e:
        logger.error(f"run_risk_analysis failed: {e}", exc_info=True)
        message = f"Error running risk analysis: {e}"
        _add_canonical_response(
            graph_state,
            tool_name="run_risk_analysis",
            message=message,
            success=False,
            priority=110,
            source="tool_exception",
        )
        return message, {}


# ─────────────────────────────────────────────────────────────────────
# 6. ITN DISTRIBUTION PLANNING
# ─────────────────────────────────────────────────────────────────────

def _load_year_specific_unified_dataset(session_folder: str, year: int,
                                        data_handler) -> bool:
    """Load unified_dataset_{year}.csv into data_handler.unified_dataset."""
    import pandas as pd
    path = os.path.join(session_folder, f'unified_dataset_{year}.csv')
    if not os.path.exists(path):
        return False
    data_handler.unified_dataset = pd.read_csv(path)
    logger.info(f"Loaded unified_dataset_{year}.csv ({len(data_handler.unified_dataset)} rows)")
    return True


@tool
def plan_itn_distribution(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    total_nets: Optional[int] = None,
    avg_household_size: Optional[float] = None,
    urban_threshold: float = 75.0,
    method: str = "composite",
    year: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Plan optimal ITN (bed net) distribution across wards.

    If total_nets or avg_household_size has not been explicitly provided by
    the user, ask for the missing values before calculating. Do not assume a
    net count or household size.

    Allocates nets using a two-tier system: rural high-risk wards first, then
    urban wards with remaining nets. Produces a map, CSV export, and
    interactive dashboard.

    Must be run AFTER risk analysis is complete.

    Args:
        thought: Your reasoning about ITN planning parameters.
        total_nets: Total number of nets available for distribution. Required
                    before calculating.
        avg_household_size: Average household size. Required before calculating.
        urban_threshold: Urban % cutoff for rural priority (default 75%).
        method: 'composite' (default) or 'pca' ranking method.
        year: Optional year (e.g. 2023) for year-specific ITN planning in
              multi-year datasets. None uses the aggregate unified_dataset.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        if year is not None:
            session_folder = os.path.join('instance', 'uploads', session_id)
            year_tag, err = _resolve_year_tag(session_id, year)
            if err:
                _add_canonical_response(
                    graph_state,
                    tool_name="plan_itn_distribution",
                    message=err,
                    success=False,
                    priority=110,
                )
                return err, {}
            from app.services.data_handler import DataHandler
            dh = DataHandler(session_folder)
            _load_year_specific_unified_dataset(session_folder, year, dh)

        from app.planning.itn_tools import PlanITNDistribution
        result = PlanITNDistribution(
            total_nets=total_nets,
            avg_household_size=avg_household_size,
            urban_threshold=urban_threshold,
            method=method,
        ).execute(session_id=session_id)
        message = result.message or "ITN distribution plan created."

        data = result.data or {}
        if not result.success:
            _add_canonical_response(
                graph_state,
                tool_name="plan_itn_distribution",
                message=message,
                success=False,
                metadata=_safe_dict(result.metadata),
                data=_safe_dict(result.data),
                priority=110,
            )
            return message, {}

        _add_viz_to_state(graph_state, data.get('file_path'))

        download_links = data.get('download_links', [])
        if download_links:
            message += "\n\n**Downloads available:**"
            for link in download_links:
                message += f"\n- [{link.get('description', 'Download')}]({link.get('url', '')})"

        _add_canonical_response(
            graph_state,
            tool_name="plan_itn_distribution",
            message=message,
            success=True,
            metadata=_safe_dict(result.metadata),
            data=_safe_dict(result.data),
            priority=105,
        )
        return message, {}
    except Exception as e:
        logger.error(f"plan_itn_distribution failed: {e}", exc_info=True)
        message = f"Error planning ITN distribution: {e}"
        _add_canonical_response(
            graph_state,
            tool_name="plan_itn_distribution",
            message=message,
            success=False,
            priority=110,
            source="tool_exception",
        )
        return message, {}


# ─────────────────────────────────────────────────────────────────────
# 7. SETTLEMENT CLASSIFICATION
# ─────────────────────────────────────────────────────────────────────

@tool
def create_settlement_classification(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    ward_names: Optional[List[str]] = None,
    ward_ids: Optional[List[str]] = None,
    top_n: Optional[int] = None,
    method: str = "composite",
    cell_size_m: int = 500,
    include_no_buildings: bool = True,
) -> Tuple[str, Dict[str, Any]]:
    """Create a manual settlement classification grid map.

    Use this when users ask to classify settlement patterns, validate wards
    against satellite imagery, create a Shiny-style formal/informal/slum grid,
    or classify top-risk wards after malaria risk analysis.

    This tool requires uploaded/enriched data and a shapefile. It does not
    require malaria risk analysis unless the user asks for top-risk wards.

    Args:
        thought: Your reasoning about which wards or rankings to use.
        ward_names: Optional ward names to classify.
        ward_ids: Optional stable ward IDs to classify.
        top_n: Optional number of top risk-ranked wards to classify.
        method: Ranking method for top_n: 'composite' or 'pca'.
        cell_size_m: Grid size in meters. Default 500.
        include_no_buildings: Include the utility "No Buildings/Avoid Area" label.
    """
    session_id = graph_state.get('session_id', 'default')
    try:
        from app.settlement import SettlementClassificationTool
        result = SettlementClassificationTool(
            ward_names=ward_names,
            ward_ids=ward_ids,
            top_n=top_n,
            method=method,
            cell_size_m=cell_size_m,
            include_no_buildings=include_no_buildings,
        ).execute(session_id=session_id)

        message = result.message or "Settlement classification map created."
        data = result.data or {}
        if not result.success:
            _add_canonical_response(
                graph_state,
                tool_name="create_settlement_classification",
                message=message,
                success=False,
                metadata=_safe_dict(result.metadata),
                data=_safe_dict(result.data),
                priority=110,
            )
            return message, {}

        _add_viz_to_state(graph_state, data.get('file_path'))
        download_links = data.get('download_links', [])
        if download_links:
            message += "\n\n**Downloads available:**"
            for link in download_links:
                message += f"\n- [{link.get('description', 'Download')}]({link.get('url', '')})"

        _add_canonical_response(
            graph_state,
            tool_name="create_settlement_classification",
            message=message,
            success=True,
            metadata=_safe_dict(result.metadata),
            data=_safe_dict(result.data),
            priority=105,
        )
        return message, {}
    except Exception as e:
        logger.error(f"create_settlement_classification failed: {e}", exc_info=True)
        message = f"Error creating settlement classification map: {e}"
        _add_canonical_response(
            graph_state,
            tool_name="create_settlement_classification",
            message=message,
            success=False,
            priority=110,
            source="tool_exception",
        )
        return message, {}


# ─────────────────────────────────────────────────────────────────────
# 8. SWITCH TPR COMBINATION
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
        from app.tpr.query_tool import SwitchTPRCombination
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
