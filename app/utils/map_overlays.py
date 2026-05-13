"""LGA boundary overlay helpers for map visualizations."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Sequence

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from shapely.geometry import LineString, MultiLineString, mapping

from .geospatial_levels import _detect_column, LGA_CODE_CANDIDATES, LGA_NAME_CANDIDATES

logger = logging.getLogger(__name__)


def calculate_lga_averages(
    gdf: gpd.GeoDataFrame,
    value_column: str,
    numerator_col: Optional[str] = None,
    denominator_col: Optional[str] = None,
    rate_multiplier: Optional[float] = None,
    cap_value: Optional[float] = None,
) -> Dict[str, float]:
    """Calculate LGA-level averages using volume-weighted method when possible.

    For rate data (TPR), uses sum(numerator)/sum(denominator).
    For other data, uses simple mean.

    Returns: {lga_code: average_value}
    """
    if gdf.empty or value_column not in gdf.columns:
        return {}

    code_col = _detect_column(gdf, LGA_CODE_CANDIDATES)
    if not code_col:
        return {}

    # Volume-weighted for rate data
    if numerator_col and denominator_col and numerator_col in gdf.columns and denominator_col in gdf.columns:
        multiplier = rate_multiplier
        if multiplier is None:
            multiplier = 1000 if 'burden' in value_column.lower() else 100
        agg = gdf.groupby(code_col).agg({
            numerator_col: 'sum',
            denominator_col: 'sum'
        })
        agg['_avg'] = (agg[numerator_col] / agg[denominator_col].replace(0, np.nan)) * multiplier
        if cap_value is not None:
            agg['_avg'] = agg['_avg'].clip(lower=0, upper=cap_value)
        return agg['_avg'].dropna().to_dict()

    # Simple mean fallback
    averages = gdf.groupby(code_col)[value_column].mean()
    if cap_value is not None:
        averages = averages.clip(lower=0, upper=cap_value)
    return averages.to_dict()


def get_lga_boundary_coords(gdf: gpd.GeoDataFrame) -> List[Dict]:
    """Extract LGA boundary coordinates for overlay lines.

    Dissolves wards to LGA, extracts boundary LineStrings.
    Returns list of {lons, lats} for each boundary segment.
    """
    code_col = _detect_column(gdf, LGA_CODE_CANDIDATES)
    if not code_col or gdf.empty:
        return []

    try:
        dissolved = gdf.dissolve(by=code_col).reset_index()
    except Exception as e:
        logger.warning(f"Failed to dissolve for LGA boundaries: {e}")
        return []

    segments = []
    for geom in dissolved.geometry:
        if geom is None or geom.is_empty:
            continue
        # Extract boundary as LineString(s)
        boundary = geom.boundary
        if boundary is None or boundary.is_empty:
            continue

        lines = [boundary] if isinstance(boundary, LineString) else list(boundary.geoms) if hasattr(boundary, 'geoms') else []
        for line in lines:
            if hasattr(line, 'coords'):
                coords = list(line.coords)
                if len(coords) >= 2:
                    segments.append({
                        'lons': [c[0] for c in coords] + [None],
                        'lats': [c[1] for c in coords] + [None]
                    })
    return segments


def add_lga_boundary_overlay(
    fig: go.Figure,
    gdf: gpd.GeoDataFrame,
    line_color: str = 'black',
    line_width: float = 2.0,
) -> None:
    """Add LGA boundary lines as overlay on a Plotly mapbox figure."""
    segments = get_lga_boundary_coords(gdf)
    if not segments:
        return

    # Combine all segments into single trace with None separators
    all_lons, all_lats = [], []
    for seg in segments:
        all_lons.extend(seg['lons'])
        all_lats.extend(seg['lats'])

    fig.add_trace(go.Scattermapbox(
        lon=all_lons,
        lat=all_lats,
        mode='lines',
        line=dict(color=line_color, width=line_width),
        hoverinfo='skip',
        showlegend=False,
        name='LGA Boundaries'
    ))


def enhance_hover_with_lga_avg(
    gdf: gpd.GeoDataFrame,
    value_column: str,
    lga_averages: Dict[str, float],
    base_hover_template: str,
    value_format: str = '.1f',
) -> List[str]:
    """Create hover text list with LGA average appended.

    Args:
        gdf: GeoDataFrame with ward data
        value_column: Column being visualized
        lga_averages: Dict from calculate_lga_averages
        base_hover_template: Base hover text per row (can use {ward}, {value}, {lga})
        value_format: Format string for the LGA average value

    Returns: List of hover strings, one per row
    """
    code_col = _detect_column(gdf, LGA_CODE_CANDIDATES)
    name_col = _detect_column(gdf, LGA_NAME_CANDIDATES)

    hover_texts = []
    for idx, row in gdf.iterrows():
        lga_code = row.get(code_col) if code_col else None
        lga_name = row.get(name_col) if name_col else (lga_code or 'Unknown')
        ward_name = row.get('WardName', row.get('ward_name', f'Ward {idx}'))
        value = row.get(value_column)

        # Format base text
        text = base_hover_template.format(
            ward=ward_name,
            value=f'{value:{value_format}}' if pd.notna(value) else 'N/A',
            lga=lga_name
        )

        # Append LGA average if available
        if lga_code and lga_code in lga_averages:
            lga_avg = lga_averages[lga_code]
            text += f"<br>LGA Avg: {lga_avg:{value_format}}"

        hover_texts.append(text)

    return hover_texts
