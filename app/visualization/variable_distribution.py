"""
Variable Distribution Visualization Tool
Creates spatial distribution maps for any variable from uploaded data
"""

import os
import re
import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
import numpy as np
from typing import Dict, Any, Optional, List, Tuple
from flask import session
import logging
from pydantic import Field, validator

from app.utils.tool_base import BaseTool, ToolCategory, ToolExecutionResult
from app.services.variable_resolver import variable_resolver
from app.utils.geospatial_levels import (
    apply_lga_highlight,
    collect_lga_options,
    dissolve_to_lga,
    normalize_lga_code,
)
from app.utils.lga_boundaries import (
    annotate_with_lga_names,
    get_reference_lga_geometries,
)
from app.utils.visualization_controls import inject_lga_hover_highlight
from app.utils.map_overlays import (
    add_lga_boundary_overlay,
    calculate_lga_averages,
)
from app.visualization.tab_html_builder import build_tabbed_html

logger = logging.getLogger(__name__)

class VariableDistribution(BaseTool):
    """Create spatial distribution maps for any variable from uploaded CSV and shapefile data"""

    variable_name: str = Field(..., description="Name of the variable to visualize (e.g., 'pfpr', 'rainfall', 'housing_quality')")
    geographic_level: str = Field(
        'ward',
        description="Geographic level for rendering: 'ward' (default) or 'lga'",
    )
    selected_lgas: Optional[List[str]] = Field(
        default=None,
        description="Optional list of LGA codes to highlight when rendering",
    )
    year_tag: str = Field(
        '',
        description="Year suffix for multi-year datasets e.g. '_2022'. Empty string uses aggregate.",
    )

    @validator('geographic_level')
    def validate_geographic_level(cls, value: str) -> str:
        value = (value or 'ward').lower()
        if value not in {'ward', 'lga'}:
            raise ValueError("geographic_level must be 'ward' or 'lga'")
        return value

    @classmethod
    def get_tool_name(cls) -> str:
        return "variable_distribution"

    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION

    @classmethod
    def get_description(cls) -> str:
        return "Map EXISTING RAW DATA COLUMNS from uploaded CSV (like TPR, rainfall, elevation, housing_quality). NOT for risk/vulnerability maps - those use create_vulnerability_map_comparison tool. This tool requires specifying an EXACT COLUMN NAME from the dataset."

    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show me the distribution of TPR variable",
            "Map the rainfall distribution",
            "Create a map of housing_quality distribution",
            "Visualize the spatial distribution of elevation",
            "Plot mean_rainfall on map"
        ]

    # ─────────────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ─────────────────────────────────────────────────────────────────

    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute variable distribution visualization"""
        try:
            session_dir = f'instance/uploads/{session_id}'
            logger.info("=" * 60)
            logger.info(f"🔍 VARIABLE DISTRIBUTION: '{self.variable_name}' | year_tag='{self.year_tag}'")
            logger.info(f"🔍 Session ID: {session_id}")

            # Detect multi-year mode: aggregate request + per-year files present
            if not self.year_tag:
                year_files = self._find_year_files(session_dir)
                logger.info(f"🔍 Per-year raw_data files found: {year_files}")
            else:
                year_files = []

            if year_files:
                return self._execute_multi_year(session_id, session_dir, year_files)
            else:
                return self._execute_single(session_id)

        except Exception as e:
            logger.error(f"Error in variable distribution visualization: {e}")
            return ToolExecutionResult(
                success=False,
                message=f"Error creating visualization: {str(e)}",
                error_details=str(e)
            )

    # ─────────────────────────────────────────────────────────────────
    # SINGLE-MAP PATH (aggregate or explicit year_tag)
    # ─────────────────────────────────────────────────────────────────

    def _execute_single(self, session_id: str) -> ToolExecutionResult:
        csv_data, shapefile_data = self._load_data(session_id)
        if csv_data is None:
            return ToolExecutionResult(
                success=False,
                message="No CSV data found. Please upload your data first.",
                error_details="Missing CSV data"
            )
        if shapefile_data is None:
            return ToolExecutionResult(
                success=False,
                message="Shapefile data required for spatial distribution maps. Please upload both CSV and shapefile data.",
                error_details="Missing shapefile data"
            )

        resolution = variable_resolver.resolve_variable(
            self.variable_name,
            list(csv_data.columns),
            threshold=0.7,
            return_suggestions=True
        )
        if not resolution['matched']:
            error_msg = variable_resolver.create_variable_error_message(
                self.variable_name, list(csv_data.columns),
                context="in the uploaded CSV data"
            )
            return ToolExecutionResult(success=False, message=error_msg,
                                       error_details=f"Variable not found: {self.variable_name}")

        resolved_variable = resolution['matched']
        if resolution['confidence'] < 1.0:
            logger.info(f"Fuzzy matched '{self.variable_name}' → '{resolved_variable}' "
                        f"({resolution['confidence']:.0%})")

        available_lgas = collect_lga_options(csv_data, shapefile_data)
        selected_lgas = self._normalize_selected_lgas(available_lgas)

        map_result = self._create_spatial_distribution_map(
            csv_data, shapefile_data, resolved_variable, session_id, selected_lgas
        )
        if not map_result:
            return ToolExecutionResult(
                success=False,
                message=f"Could not create spatial map for {self.variable_name}",
                error_details="Map generation failed"
            )

        stats_text = self._generate_statistics(csv_data, resolved_variable)
        self._track_exploration_activity(session_id, resolved_variable)
        workflow_guidance = self._generate_workflow_guidance(session_id)

        if resolution['confidence'] < 1.0:
            response_text = f"**{resolved_variable.upper()} Spatial Distribution** (matched from '{self.variable_name}')\n\n{stats_text}"
        else:
            response_text = f"**{resolved_variable.upper()} Spatial Distribution**\n\n{stats_text}"
        response_text += f"\n\nI've created a spatial distribution map showing how **{resolved_variable}** varies across your study area."

        if workflow_guidance.get('show_guidance', False):
            response_text += f"\n\n**💡 Next Steps**\n{workflow_guidance['message']}"

        return ToolExecutionResult(
            success=True,
            message=response_text,
            data={
                'variable': self.variable_name,
                'total_records': len(csv_data),
                'web_path': map_result['web_path'],
                'chart_type': 'spatial_distribution_map',
                'file_path': map_result['file_path'],
                'workflow_guidance': workflow_guidance,
                'geographic_level': self.geographic_level,
                'selected_lgas': selected_lgas,
                'available_lgas': available_lgas,
            }
        )

    # ─────────────────────────────────────────────────────────────────
    # MULTI-YEAR PATH
    # ─────────────────────────────────────────────────────────────────

    def _execute_multi_year(self, session_id: str, session_dir: str,
                            years: List[int]) -> ToolExecutionResult:
        """Build one choropleth per year + aggregate, embed all in a single tabbed HTML."""
        shapefile_data = self._load_shapefile(session_id)
        if shapefile_data is None:
            return ToolExecutionResult(
                success=False,
                message="Shapefile data required for spatial distribution maps.",
                error_details="Missing shapefile data"
            )

        year_figures: List[Tuple[int, go.Figure, gpd.GeoDataFrame, str]] = []
        resolved_variable = None

        for year in years:
            csv_path = os.path.join(session_dir, f'raw_data_{year}.csv')
            try:
                csv_data = pd.read_csv(csv_path)
            except Exception as e:
                logger.warning(f"Skipping year {year}: cannot read CSV — {e}")
                continue

            resolution = variable_resolver.resolve_variable(
                self.variable_name, list(csv_data.columns),
                threshold=0.7, return_suggestions=True
            )
            if not resolution['matched']:
                logger.warning(f"Variable '{self.variable_name}' not found in year {year} CSV")
                continue

            if resolved_variable is None:
                resolved_variable = resolution['matched']

            available_lgas = collect_lga_options(csv_data, shapefile_data)
            selected_lgas = self._normalize_selected_lgas(available_lgas)

            result = self._build_map_figure(csv_data, shapefile_data, resolved_variable,
                                            session_id, selected_lgas)
            if result is None:
                logger.warning(f"Figure build failed for year {year}")
                continue

            fig, plot_data, plot_level = result
            fig.update_layout(title={
                'text': f'{resolved_variable.upper()} Distribution — {year}',
                'x': 0.5, 'xanchor': 'center',
                'font': {'size': 18, 'color': '#2E3440'}
            })
            year_figures.append((year, fig, plot_data, plot_level))

        if not year_figures:
            return ToolExecutionResult(
                success=False,
                message=f"Could not build maps for variable '{self.variable_name}' across any year.",
                error_details="No figures generated"
            )

        # Build aggregate 'All Years' tab from raw_data.csv
        tabs = []
        agg_csv_path = os.path.join(session_dir, 'raw_data.csv')
        if os.path.exists(agg_csv_path):
            try:
                agg_data = pd.read_csv(agg_csv_path)
                agg_available_lgas = collect_lga_options(agg_data, shapefile_data)
                agg_selected_lgas = self._normalize_selected_lgas(agg_available_lgas)
                agg_result = self._build_map_figure(
                    agg_data, shapefile_data, resolved_variable, session_id, agg_selected_lgas
                )
                if agg_result is not None:
                    agg_fig, _, _ = agg_result
                    agg_fig.update_layout(title={
                        'text': f'{resolved_variable.upper()} Distribution — All Years (Aggregate)',
                        'x': 0.5, 'xanchor': 'center',
                        'font': {'size': 18, 'color': '#2E3440'}
                    })
                    tabs.append(('agg', 'All Years', agg_fig))
            except Exception as e:
                logger.warning(f"Could not build aggregate tab: {e}")

        for year, fig, _, _ in year_figures:
            tabs.append((year, str(year), fig))

        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{resolved_variable}_multi_year_distribution_{timestamp}.html"
        save_result = build_tabbed_html(
            tabs=tabs,
            nav_label=f"{resolved_variable.upper()} by year:",
            filename=filename,
            session_id=session_id,
        )

        stats_lines = []
        if os.path.exists(agg_csv_path):
            try:
                df = pd.read_csv(agg_csv_path)
                vals = df[resolved_variable].dropna()
                stats_lines.append(f"**All Years:** mean {vals.mean():.1f}, range [{vals.min():.1f}–{vals.max():.1f}]")
            except Exception:
                pass
        for year, *_ in year_figures:
            csv_path = os.path.join(session_dir, f'raw_data_{year}.csv')
            try:
                df = pd.read_csv(csv_path)
                vals = df[resolved_variable].dropna()
                stats_lines.append(f"**{year}:** mean {vals.mean():.1f}, range [{vals.min():.1f}–{vals.max():.1f}]")
            except Exception:
                pass

        years_shown = [y for y, *_ in year_figures]
        agg_note = " (includes 'All Years' aggregate tab)" if any(k == 'agg' for k, *_ in tabs) else ""
        response_text = (
            f"**{resolved_variable.upper()} — Year-by-Year Distribution** "
            f"({len(years_shown)} years: {min(years_shown)}–{max(years_shown)}){agg_note}\n\n"
            + "\n".join(stats_lines)
            + f"\n\nUse the year buttons at the top of the map to navigate between years."
        )

        return ToolExecutionResult(
            success=True,
            message=response_text,
            data={
                'variable': resolved_variable,
                'years': years_shown,
                'web_path': save_result['web_path'],
                'chart_type': 'multi_year_distribution_map',
                'file_path': save_result['file_path'],
                'geographic_level': self.geographic_level,
            }
        )

    # ─────────────────────────────────────────────────────────────────
    # FIGURE BUILDER (shared by single and multi-year paths)
    # ─────────────────────────────────────────────────────────────────

    def _build_map_figure(
        self,
        csv_data: pd.DataFrame,
        shapefile: gpd.GeoDataFrame,
        variable: str,
        session_id: str,
        selected_lgas: List[str],
    ) -> Optional[Tuple[go.Figure, gpd.GeoDataFrame, str]]:
        """Build and return (fig, plot_data, plot_level) without saving to disk."""
        try:
            # Merge CSV with shapefile
            join_columns = ['WardCode', 'LGACode', 'WardName', 'ward_code', 'ward_name']
            merged_data = None
            for col in join_columns:
                if col in csv_data.columns and col in shapefile.columns:
                    try:
                        merged_data = shapefile.merge(
                            csv_data, on=col, how='left',
                            suffixes=('_shp', '')
                        )
                        logger.info(f"Merged on column: {col}")
                        break
                    except Exception as e:
                        logger.warning(f"Failed to merge on {col}: {e}")
                        continue

            if merged_data is None:
                logger.error("Could not merge CSV and shapefile data")
                return None

            if variable not in merged_data.columns:
                logger.error(f"Variable {variable} not found in merged data")
                return None

            # Wards with no value but valid geometry — rendered as grey "no data" trace
            no_data_mask = merged_data[variable].isna() & merged_data.geometry.notnull() & ~merged_data.geometry.is_empty
            no_data = merged_data[no_data_mask].copy()

            clean_data = merged_data.dropna(subset=[variable])
            valid_mask = clean_data.geometry.notnull() & ~clean_data.geometry.is_empty
            dropped = (~valid_mask).sum()
            if dropped:
                logger.warning(f"Skipping {dropped} records with missing geometry")
            clean_data = clean_data[valid_mask]
            if clean_data.empty:
                logger.error(f"All records lost after geometry filtering for {variable}")
                return None

            try:
                clean_data = annotate_with_lga_names(clean_data)
            except Exception as exc:
                logger.warning(f"Failed to annotate LGA names: {exc}")

            from shapely.geometry import LineString, MultiLineString, Polygon

            def convert_to_polygon(geom):
                if isinstance(geom, (LineString, MultiLineString)):
                    try:
                        if isinstance(geom, LineString) and geom.is_ring:
                            return Polygon(geom)
                        return geom.buffer(0.001)
                    except Exception:
                        return geom
                return geom

            clean_data['geometry'] = clean_data.geometry.apply(convert_to_polygon)

            # Color scale
            if variable.lower() in ['pfpr', 'tpr', 'u5_tpr_rdt']:
                color_scale = 'Reds'
                title_suffix = 'Risk Distribution'
            elif 'rainfall' in variable.lower() or 'ndvi' in variable.lower():
                color_scale = 'Blues'
                title_suffix = 'Environmental Distribution'
            else:
                color_scale = 'Viridis'
                title_suffix = 'Spatial Distribution'

            available_lgas = collect_lga_options(clean_data)
            plot_level = self.geographic_level
            plot_data = clean_data.copy()
            highlight_codes = selected_lgas

            if plot_level == 'lga':
                try:
                    aggregated = (
                        clean_data.groupby('LGACode', dropna=True)
                        .agg({variable: 'mean', 'StateName': 'first', 'LGAName': 'first'})
                        .reset_index()
                    )
                    reference_shapes = get_reference_lga_geometries(
                        aggregated[['LGACode', 'StateName', 'LGAName']]
                    )
                    if reference_shapes is not None and not reference_shapes.empty:
                        plot_data = reference_shapes.merge(
                            aggregated[['LGACode', variable]], on='LGACode', how='left'
                        )
                    else:
                        raise ValueError('No matching LGA boundaries found')
                except Exception as agg_err:
                    logger.error(f"Failed reference LGA polygons: {agg_err}")
                    try:
                        plot_data = dissolve_to_lga(clean_data, value_columns=[variable])
                    except Exception as dissolve_err:
                        logger.error(f"Failed fallback dissolve: {dissolve_err}")
                        plot_data = clean_data.copy()
                        plot_level = 'ward'

            plot_data = apply_lga_highlight(plot_data, highlight_codes, 'LGACode')

            # Variable label/unit mapping
            var_lower = variable.lower()
            if 'burden' in var_lower:
                var_label, var_unit = "Malaria Burden", " per 1,000"
            elif 'tpr' in var_lower or 'positivity' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), "%"
            elif 'pfpr' in var_lower or 'parasite' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), "%"
            elif 'population' in var_lower and 'density' not in var_lower:
                var_label, var_unit = "Population", ""
            elif 'distance' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), " m"
            elif 'elevation' in var_lower or 'altitude' in var_lower or var_lower in ['ele', 'dem']:
                var_label, var_unit = variable.replace('_', ' ').title(), " m"
            elif 'rainfall' in var_lower or 'precipitation' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), " mm"
            elif 'temperature' in var_lower or 'temp' in var_lower or 'lst' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), " °C"
            elif 'humidity' in var_lower or 'wetness' in var_lower or 'soil' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), "%"
            elif 'density' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), " per km²"
            elif 'flood' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), "%"
            elif 'ntl' in var_lower or 'night' in var_lower or 'light' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), " nW/cm²/sr"
            elif 'housing' in var_lower:
                var_label, var_unit = variable.replace('_', ' ').title(), " (index)"
            elif any(x in var_lower for x in ['evi', 'ndvi', 'ndmi', 'ndwi']):
                var_label, var_unit = variable.replace('_', ' ').upper(), ""
            else:
                var_label, var_unit = variable.replace('_', ' ').title(), ""

            colorbar_title = f"{var_label}{var_unit}" if var_unit else var_label

            if 'burden' in var_lower and variable in clean_data.columns:
                clean_data[variable] = pd.to_numeric(clean_data[variable], errors='coerce').clip(lower=0, upper=1000)
                if plot_level == 'lga' and variable in plot_data.columns:
                    plot_data[variable] = pd.to_numeric(plot_data[variable], errors='coerce').clip(lower=0, upper=1000)

            lga_averages = {}
            if plot_level == 'ward':
                lga_averages = calculate_lga_averages(clean_data, variable)

            from shapely.geometry import mapping

            def build_geojson(df: gpd.GeoDataFrame):
                features = []
                for idx, row in df.iterrows():
                    features.append({
                        'type': 'Feature',
                        'id': str(idx),
                        'geometry': mapping(row.geometry),
                        'properties': {}
                    })
                return {'type': 'FeatureCollection', 'features': features}

            def build_hover_text(df):
                texts = []
                for idx, row in df.iterrows():
                    ward_name = row.get('WardName') or row.get('ward_name') or str(idx)
                    lga_name = row.get('LGAName') or row.get('lga_name') or 'Unknown'
                    val = row.get(variable)
                    lga_code = row.get('LGACode')
                    is_imputed = bool(row.get('_imputed', False))

                    text = f"<b>Ward:</b> {ward_name}<br><b>LGA:</b> {lga_name}<br>"
                    if pd.notna(val):
                        if abs(val) >= 1000:
                            val_str = f"{val:,.0f}"
                        elif abs(val) >= 10:
                            val_str = f"{val:.1f}"
                        else:
                            val_str = f"{val:.2f}"
                        text += f"<br><b>{var_label}:</b> {val_str}{var_unit}"
                        if is_imputed:
                            text += "<br><i style='color:#e67e22'>⚠ No data for this year — showing aggregate value</i>"
                        if lga_code and lga_code in lga_averages:
                            lga_avg = lga_averages[lga_code]
                            diff = val - lga_avg
                            diff_sign = '+' if diff > 0 else ''
                            diff_color = '#e74c3c' if diff > 0 else '#27ae60' if diff < 0 else '#666'
                            avg_str = (f"{lga_avg:,.0f}" if abs(lga_avg) >= 1000
                                       else f"{lga_avg:.1f}" if abs(lga_avg) >= 10
                                       else f"{lga_avg:.2f}")
                            text += (f"<br><b>LGA Average:</b> {avg_str}{var_unit}"
                                     f" <span style='color:{diff_color}'>({diff_sign}{diff:.1f})</span>")
                    else:
                        text += f"<br><b>{var_label}:</b> No data"
                    texts.append(text)
                return texts

            fig = go.Figure()

            def add_trace(df, show_scale, opacity, colorscale_override=None, name_suffix=''):
                if df.empty:
                    return
                geojson = build_geojson(df)
                hover_texts = build_hover_text(df)
                fig.add_trace(go.Choroplethmapbox(
                    geojson=geojson,
                    locations=df.index.astype(str),
                    z=df[variable],
                    colorscale=colorscale_override or color_scale,
                    hovertext=hover_texts,
                    hovertemplate='%{hovertext}<extra></extra>',
                    marker_opacity=opacity,
                    marker_line_width=1,
                    marker_line_color='white',
                    showscale=show_scale,
                    colorbar=dict(title=colorbar_title, thickness=15, len=0.7) if show_scale else None,
                    name=name_suffix or variable
                ))

            if highlight_codes:
                faded = plot_data[~plot_data['_is_selected_lga']]
                highlighted = plot_data[plot_data['_is_selected_lga']]
                add_trace(faded, show_scale=False, opacity=0.25,
                          colorscale_override=[[0, '#d1d5db'], [1, '#9ca3af']],
                          name_suffix='Other LGAs')
                add_trace(highlighted, show_scale=True, opacity=0.85, name_suffix='Selected LGA')
            else:
                # Single trace — imputed wards distinguished in hover text only,
                # not by splitting traces (two Choroplethmapbox traces break colorscale)
                add_trace(plot_data, show_scale=True, opacity=0.75)

            if plot_level == 'ward':
                add_lga_boundary_overlay(fig, clean_data)

            bounds = plot_data.total_bounds
            center_lat = (bounds[1] + bounds[3]) / 2
            center_lon = (bounds[0] + bounds[2]) / 2

            fig.update_layout(
                title={
                    'text': f'{variable.upper()} {title_suffix}',
                    'x': 0.5, 'xanchor': 'center',
                    'font': {'size': 18, 'color': '#2E3440'}
                },
                mapbox=dict(
                    style='open-street-map',
                    center=dict(lat=center_lat, lon=center_lon),
                    zoom=8
                ),
                height=600,
                margin=dict(t=60, b=20, l=20, r=20),
                template='plotly_white'
            )

            return fig, plot_data, plot_level

        except Exception as e:
            logger.error(f"Error building map figure: {e}")
            return None

    # ─────────────────────────────────────────────────────────────────
    # HTML SAVERS
    # ─────────────────────────────────────────────────────────────────

    def _save_single_figure(self, fig: go.Figure, plot_data: gpd.GeoDataFrame,
                            plot_level: str, variable: str,
                            session_id: str) -> Dict[str, str]:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{variable}_distribution_map_{timestamp}.html"

        try:
            from flask import current_app
            upload_folder = current_app.config['UPLOAD_FOLDER']
        except RuntimeError:
            upload_folder = os.path.join('instance', 'uploads')

        session_dir = os.path.join(upload_folder, session_id)
        os.makedirs(session_dir, exist_ok=True)
        file_path = os.path.join(session_dir, filename)
        fig.write_html(file_path)

        if plot_level == 'ward':
            try:
                lga_codes = plot_data['LGACode'].fillna('').astype(str).tolist()
                inject_lga_hover_highlight(file_path, lga_codes)
            except Exception as hover_err:
                logger.warning(f"Failed to inject LGA hover highlight: {hover_err}")

        return {
            'file_path': file_path,
            'web_path': f"/serve_viz_file/{session_id}/{filename}",
        }

    # ─────────────────────────────────────────────────────────────────
    # _create_spatial_distribution_map (single-map, backward compat)
    # ─────────────────────────────────────────────────────────────────

    def _create_spatial_distribution_map(
        self,
        csv_data: pd.DataFrame,
        shapefile: gpd.GeoDataFrame,
        variable: str,
        session_id: str,
        selected_lgas: List[str],
    ) -> Optional[Dict[str, Any]]:
        result = self._build_map_figure(csv_data, shapefile, variable, session_id, selected_lgas)
        if result is None:
            return None
        fig, plot_data, plot_level = result
        save = self._save_single_figure(fig, plot_data, plot_level, variable, session_id)
        return {
            'type': 'spatial_distribution_map',
            'title': f'{variable.upper()} Spatial Distribution',
            'file_path': save['file_path'],
            'web_path': save['web_path'],
            'description': f'Spatial distribution map of {variable} across study area',
        }

    # ─────────────────────────────────────────────────────────────────
    # DATA LOADERS
    # ─────────────────────────────────────────────────────────────────

    def _find_year_files(self, session_dir: str) -> List[int]:
        """Return sorted list of years that have raw_data_{year}.csv files."""
        years = []
        if not os.path.isdir(session_dir):
            return years
        for fname in os.listdir(session_dir):
            m = re.match(r'^raw_data_(\d{4})\.csv$', fname)
            if m:
                years.append(int(m.group(1)))
        return sorted(years)

    def _load_shapefile(self, session_id: str) -> Optional[gpd.GeoDataFrame]:
        """Load shapefile from session folder."""
        upload_dir = os.path.join('instance', 'uploads', session_id)
        zip_path = os.path.join(upload_dir, 'raw_shapefile.zip')
        if not os.path.exists(zip_path):
            return None
        try:
            import zipfile
            shapefile_dir = os.path.join(upload_dir, 'shapefile')
            os.makedirs(shapefile_dir, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(shapefile_dir)
            shp_files = [f for f in os.listdir(shapefile_dir) if f.endswith('.shp')]
            if shp_files:
                return gpd.read_file(os.path.join(shapefile_dir, shp_files[0]))
        except Exception as e:
            logger.error(f"Error loading shapefile: {e}")
        return None

    def _load_data(self, session_id: str) -> tuple:
        """Load CSV (respecting year_tag) and shapefile from session."""
        logger.info("🔍 _load_data: Starting data load")
        try:
            upload_dir = os.path.join('instance', 'uploads', session_id)

            csv_data = None
            csv_path = os.path.join(upload_dir, f'raw_data{self.year_tag}.csv')
            if os.path.exists(csv_path):
                csv_data = pd.read_csv(csv_path)
                logger.info(f"🔍 CSV loaded: {csv_data.shape}, columns: {list(csv_data.columns)[:10]}")

            shapefile_data = self._load_shapefile(session_id)
            if shapefile_data is not None:
                logger.info(f"Loaded shapefile: {len(shapefile_data)} features")

            return csv_data, shapefile_data

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None, None

    # ─────────────────────────────────────────────────────────────────
    # STATISTICS & WORKFLOW HELPERS
    # ─────────────────────────────────────────────────────────────────

    def _generate_statistics(self, data: pd.DataFrame, variable: str) -> str:
        try:
            values = data[variable].dropna()
            stats = {
                'count': len(values),
                'mean': values.mean(),
                'median': values.median(),
                'std': values.std(),
                'min': values.min(),
                'max': values.max(),
                'missing': data[variable].isna().sum()
            }
            if variable.lower() in ['pfpr', 'tpr', 'u5_tpr_rdt']:
                return (f"**Summary Statistics:**\n"
                        f"• **Records:** {stats['count']:,} wards ({stats['missing']} missing)\n"
                        f"• **Average:** {stats['mean']:.2f}%\n"
                        f"• **Range:** {stats['min']:.2f}% to {stats['max']:.2f}%\n"
                        f"• **Standard deviation:** {stats['std']:.2f}\n"
                        f"• **Median:** {stats['median']:.2f}%")
            else:
                return (f"**Summary Statistics:**\n"
                        f"• **Records:** {stats['count']:,} wards ({stats['missing']} missing)\n"
                        f"• **Average:** {stats['mean']:.2f}\n"
                        f"• **Range:** {stats['min']:.2f} to {stats['max']:.2f}\n"
                        f"• **Standard deviation:** {stats['std']:.2f}\n"
                        f"• **Median:** {stats['median']:.2f}")
        except Exception as e:
            logger.error(f"Error generating statistics: {e}")
            return f"Statistics for {variable}: {len(data)} records"

    def _track_exploration_activity(self, session_id: str, variable_name: str):
        try:
            try:
                from flask import session as flask_session
                if 'exploration_activity' not in flask_session:
                    flask_session['exploration_activity'] = {
                        'distributions_viewed': [],
                        'exploration_count': 0,
                        'started_at': None
                    }
                if variable_name not in flask_session['exploration_activity']['distributions_viewed']:
                    flask_session['exploration_activity']['distributions_viewed'].append(variable_name)
                    flask_session['exploration_activity']['exploration_count'] += 1
                    if flask_session['exploration_activity']['started_at'] is None:
                        from datetime import datetime
                        flask_session['exploration_activity']['started_at'] = datetime.now().isoformat()
            except RuntimeError:
                logger.info(f"📊 Variable distribution created: {variable_name} (no Flask context)")
        except Exception as e:
            logger.warning(f"Failed to track exploration activity: {e}")

    def _generate_workflow_guidance(self, session_id: str) -> dict:
        try:
            try:
                from flask import session as flask_session
                exploration = flask_session.get('exploration_activity', {})
                distributions_viewed = exploration.get('distributions_viewed', [])
                exploration_count = exploration.get('exploration_count', 0)
                analysis_complete = flask_session.get('comprehensive_analysis_complete', False)
            except RuntimeError:
                exploration_count, distributions_viewed, analysis_complete = 0, [], False

            if exploration_count == 0:
                return {'show_guidance': False, 'message': '', 'phase': 'initial'}
            elif exploration_count == 1:
                return {
                    'show_guidance': True,
                    'message': "Great start exploring your data! Feel free to view more variable distributions, or when you're ready, I can run the comprehensive malaria risk analysis.",
                    'phase': 'early_exploration',
                    'suggestion': 'continue_exploring'
                }
            elif exploration_count >= 2 and not analysis_complete:
                return {
                    'show_guidance': True,
                    'message': f"You've explored {exploration_count} variables ({', '.join(distributions_viewed)}). Ready to run the comprehensive analysis to identify high-risk areas and generate vulnerability rankings?",
                    'phase': 'ready_for_analysis',
                    'suggestion': 'run_analysis',
                    'call_to_action': "Say 'run comprehensive analysis' or 'proceed with analysis' when you're ready!"
                }
            elif analysis_complete:
                return {
                    'show_guidance': True,
                    'message': "Analysis complete! You can continue exploring specific variables or ask questions about the results.",
                    'phase': 'post_analysis',
                    'suggestion': 'continue_exploration'
                }
            return {'show_guidance': False, 'message': '', 'phase': 'unknown'}
        except Exception as e:
            logger.warning(f"Failed to generate workflow guidance: {e}")
            return {'show_guidance': False, 'message': '', 'phase': 'error'}

    def _normalize_selected_lgas(self, available_lgas: List[Dict[str, str]]) -> List[str]:
        if not self.selected_lgas:
            return []
        available_codes = {normalize_lga_code(item['code']) for item in available_lgas}
        normalized = []
        for code in self.selected_lgas:
            norm = normalize_lga_code(code)
            if norm and (not available_codes or norm in available_codes):
                normalized.append(norm)
        return normalized
