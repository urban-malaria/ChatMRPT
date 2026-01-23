"""
Variable Distribution Visualization Tool
Creates spatial distribution maps for any variable from uploaded data
"""

import os
import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
import numpy as np
from typing import Dict, Any, Optional, List
from flask import session
import logging
from pydantic import Field, validator

from app.tools.base import BaseTool, ToolCategory, ToolExecutionResult
from app.services.variable_resolution_service import variable_resolver
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
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute variable distribution visualization"""
        try:
            # 🔍 DEBUG: Variable Distribution Execution
            logger.info("=" * 60)
            logger.info(f"🔍 DEBUG VARIABLE DISTRIBUTION: Starting for variable '{self.variable_name}'")
            logger.info(f"🔍 Session ID: {session_id}")
            
            # Check what data files exist
            session_dir = f'instance/uploads/{session_id}'
            raw_csv = os.path.join(session_dir, 'raw_data.csv')
            unified_csv = os.path.join(session_dir, 'unified_dataset.csv')
            shapefile_zip = os.path.join(session_dir, 'raw_shapefile.zip')
            
            logger.info(f"🔍 Checking data files:")
            logger.info(f"🔍   raw_data.csv: {'EXISTS' if os.path.exists(raw_csv) else 'NOT FOUND'}")
            logger.info(f"🔍   unified_dataset.csv: {'EXISTS' if os.path.exists(unified_csv) else 'NOT FOUND'}")
            logger.info(f"🔍   raw_shapefile.zip: {'EXISTS' if os.path.exists(shapefile_zip) else 'NOT FOUND'}")
            logger.info("=" * 60)
            logger.info(f"🗺️ Creating distribution map for variable: {self.variable_name}")
            
            # Load data
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
            
            # Validate and resolve variable using intelligent matching
            resolution = variable_resolver.resolve_variable(
                self.variable_name, 
                list(csv_data.columns),
                threshold=0.7,
                return_suggestions=True
            )
            
            if not resolution['matched']:
                # Create helpful error message with suggestions
                error_msg = variable_resolver.create_variable_error_message(
                    self.variable_name,
                    list(csv_data.columns),
                    context="in the uploaded CSV data"
                )
                return ToolExecutionResult(
                    success=False,
                    message=error_msg,
                    error_details=f"Variable not found: {self.variable_name}"
                )
            
            # Use the resolved variable name
            resolved_variable = resolution['matched']
            if resolution['confidence'] < 1.0:
                logger.info(f"Using fuzzy matched variable: '{self.variable_name}' → '{resolved_variable}' "
                           f"(confidence: {resolution['confidence']:.0%})")
            
            available_lgas = collect_lga_options(csv_data, shapefile_data)
            selected_lgas = self._normalize_selected_lgas(available_lgas)

            # Create spatial distribution map
            map_result = self._create_spatial_distribution_map(
                csv_data,
                shapefile_data,
                resolved_variable,
                session_id,
                selected_lgas,
            )
            if not map_result:
                return ToolExecutionResult(
                    success=False,
                    message=f"Could not create spatial map for {self.variable_name}",
                    error_details="Map generation failed"
                )

            # Generate summary statistics
            stats_text = self._generate_statistics(csv_data, resolved_variable)
            
            # Track distribution viewing for workflow awareness
            self._track_exploration_activity(session_id, resolved_variable)
            workflow_guidance = self._generate_workflow_guidance(session_id)
            
            # Generate response - show both user's variable and resolved name if different
            display_name = resolved_variable
            if resolution['confidence'] < 1.0:
                response_text = f"**{resolved_variable.upper()} Spatial Distribution** (matched from '{self.variable_name}')\n\n{stats_text}"
            else:
                response_text = f"**{resolved_variable.upper()} Spatial Distribution**\n\n{stats_text}"
            response_text += f"\n\nI've created a spatial distribution map showing how **{resolved_variable}** varies across your study area."
            
            # Visualization will be rendered by frontend using web_path
            
            # Add workflow guidance if appropriate
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
            
        except Exception as e:
            logger.error(f"Error in variable distribution visualization: {e}")
            return ToolExecutionResult(
                success=False,
                message=f"Error creating visualization: {str(e)}",
                error_details=str(e)
            )
    
    def _load_data(self, session_id: str) -> tuple[Optional[pd.DataFrame], Optional[gpd.GeoDataFrame]]:
        # 🔍 DEBUG: Loading data for variable distribution
        logger.info("🔍 DEBUG _load_data: Starting data load")
        logger.info(f"🔍 Session ID: {session_id}")
        """Load CSV and shapefile data from session"""
        try:
            upload_dir = os.path.join('instance', 'uploads', session_id)
            
            # Load raw CSV data
            csv_data = None
            csv_path = os.path.join(upload_dir, 'raw_data.csv')
            if os.path.exists(csv_path):
                csv_data = pd.read_csv(csv_path)
            logger.info(f"🔍 DEBUG: CSV loaded successfully")
            logger.info(f"🔍   Shape: {csv_data.shape}")
            logger.info(f"🔍   Columns (first 10): {list(csv_data.columns)[:10]}")
            logger.info(f"🔍   ALL Columns: {list(csv_data.columns)}")
            if self.variable_name in csv_data.columns:
                logger.info(f"🔍   ✅ Variable '{self.variable_name}' FOUND in CSV")
            else:
                logger.error(f"🔍   ❌ Variable '{self.variable_name}' NOT in CSV")
                # Check case-insensitive
                matching = [col for col in csv_data.columns if col.lower() == self.variable_name.lower()]
                if matching:
                    logger.info(f"🔍   💡 Case-insensitive match found: {matching}")
                logger.info(f"Loaded raw CSV data: {len(csv_data)} rows, {len(csv_data.columns)} columns")
            
            # Load raw shapefile data
            shapefile_data = None
            zip_path = os.path.join(upload_dir, 'raw_shapefile.zip')
            if os.path.exists(zip_path):
                import zipfile
                shapefile_dir = os.path.join(upload_dir, 'shapefile')
                os.makedirs(shapefile_dir, exist_ok=True)
                
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(shapefile_dir)
                logger.info(f"Extracted raw shapefile from {zip_path}")
                
                # Find .shp file
                shp_files = [f for f in os.listdir(shapefile_dir) if f.endswith('.shp')]
                if shp_files:
                    shp_path = os.path.join(shapefile_dir, shp_files[0])
                    shapefile_data = gpd.read_file(shp_path)
                    logger.info(f"Loaded shapefile data: {len(shapefile_data)} features")
                    logger.info(f"🔍 DEBUG: Shapefile columns: {list(shapefile_data.columns)}")
            
            return csv_data, shapefile_data
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None, None
    
    def _create_spatial_distribution_map(
        self,
        csv_data: pd.DataFrame,
        shapefile: gpd.GeoDataFrame,
        variable: str,
        session_id: str,
        selected_lgas: List[str],
    ) -> Optional[Dict[str, Any]]:
        """Create spatial distribution choropleth map like vulnerability/composite score maps"""
        try:
            # Merge CSV data with shapefile
            join_columns = ['WardName', 'WardCode', 'LGACode', 'ward_name', 'ward_code']
            
            merged_data = None
            join_col = None
            
            for col in join_columns:
                if col in csv_data.columns and col in shapefile.columns:
                    try:
                        # Merge preserving all columns with suffixes to avoid conflicts
                        merged_data = shapefile.merge(
                            csv_data, 
                            on=col, 
                            how='left',
                            suffixes=('_shp', '')  # Keep CSV columns unchanged, suffix shapefile duplicates
                        )
                        join_col = col
                        logger.info(f"Successfully merged data on column: {col}")
                        break
                    except Exception as e:
                        logger.warning(f"Failed to merge on {col}: {e}")
                        continue
            
            if merged_data is None:
                logger.error("Could not merge CSV and shapefile data")
                return None
            
            # Debug: Log merged data columns
            logger.info(f"🔍 DEBUG: Merged data shape: {merged_data.shape}")
            logger.info(f"🔍 DEBUG: Merged data columns: {list(merged_data.columns)}")
            logger.info(f"🔍 DEBUG: Looking for variable '{variable}' in merged data...")
            
            # Ensure variable has valid data
            if variable not in merged_data.columns:
                logger.error(f"Variable {variable} not found in merged data")
                logger.error(f"🔍 DEBUG: Available columns after merge: {list(merged_data.columns)}")
                return None
            
            # Remove rows with missing values for the variable
            clean_data = merged_data.dropna(subset=[variable])
            if len(clean_data) == 0:
                logger.error(f"No valid data for variable {variable}")
                return None

            # Filter out rows without usable geometries before rendering.
            valid_geometry_mask = clean_data.geometry.notnull() & ~clean_data.geometry.is_empty
            dropped_rows = (~valid_geometry_mask).sum()
            if dropped_rows:
                logger.warning(
                    "Skipping %s records with missing geometry while building %s map",
                    dropped_rows,
                    variable,
                )
            clean_data = clean_data[valid_geometry_mask]
            if clean_data.empty:
                logger.error(
                    "All records lost after geometry filtering for variable %s; aborting map render",
                    variable,
                )
                return None

            # Enrich with LGA names/state names from the national boundary reference
            try:
                clean_data = annotate_with_lga_names(clean_data)
            except Exception as exc:
                logger.warning("Failed to annotate LGA names: %s", exc)

            # Convert LineString geometries to Polygons by buffering
            # Choropleth maps require Polygon geometries, but some shapefiles have LineStrings
            from shapely.geometry import LineString, MultiLineString, Polygon, MultiPolygon

            def convert_to_polygon(geom):
                """Convert LineString to Polygon if needed"""
                if isinstance(geom, (LineString, MultiLineString)):
                    # Try to create a polygon from the linestring
                    try:
                        if isinstance(geom, LineString) and geom.is_ring:
                            # If it's a closed ring, create polygon directly
                            return Polygon(geom)
                        else:
                            # Buffer the line slightly to create a polygon
                            # Use a very small buffer (0.001 degrees ~100m)
                            return geom.buffer(0.001)
                    except:
                        logger.warning(f"Could not convert linestring to polygon")
                        return geom
                return geom

            # Apply conversion
            clean_data['geometry'] = clean_data.geometry.apply(convert_to_polygon)
            
            # Determine color scale based on variable type
            if variable.lower() in ['pfpr', 'tpr', 'u5_tpr_rdt']:
                color_scale = 'Reds'  # Risk variables use red scale
                title_suffix = 'Risk Distribution'
            elif 'rainfall' in variable.lower() or 'ndvi' in variable.lower():
                color_scale = 'Blues'  # Environmental variables use blue/green
                title_suffix = 'Environmental Distribution'
            else:
                color_scale = 'Viridis'  # Default color scale
                title_suffix = 'Spatial Distribution'

            available_lgas = collect_lga_options(clean_data)

            plot_level = self.geographic_level
            plot_data = clean_data.copy()
            highlight_codes = selected_lgas

            if plot_level == 'lga':
                try:
                    aggregated = (
                        clean_data.groupby('LGACode', dropna=True)
                        .agg({
                            variable: 'mean',
                            'StateName': 'first',
                            'LGAName': 'first',
                        })
                        .reset_index()
                    )
                    reference_shapes = get_reference_lga_geometries(aggregated[['LGACode', 'StateName', 'LGAName']])
                    if reference_shapes is not None and not reference_shapes.empty:
                        plot_data = reference_shapes.merge(
                            aggregated[['LGACode', variable]],
                            on='LGACode',
                            how='left',
                        )
                    else:
                        raise ValueError('No matching LGA boundaries found')
                except Exception as agg_err:
                    logger.error(f"Failed to use reference LGA polygons: {agg_err}")
                    try:
                        plot_data = dissolve_to_lga(clean_data, value_columns=[variable])
                    except Exception as dissolve_err:
                        logger.error(f"Failed fallback dissolve: {dissolve_err}")
                        plot_data = clean_data.copy()
                        plot_level = 'ward'

            plot_data = apply_lga_highlight(plot_data, highlight_codes, 'LGACode')

            # Calculate LGA averages for enhanced hover (ward level only)
            lga_averages = {}
            if plot_level == 'ward':
                lga_averages = calculate_lga_averages(clean_data, variable)

            # Create choropleth map using plotly graph_objects (like other ChatMRPT maps)
            fig = go.Figure()

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
                """Build hover text with ward name, LGA name, and LGA average."""
                texts = []

                # Variable-specific formatting
                var_lower = variable.lower()
                if 'burden' in var_lower:
                    var_label = "Malaria Burden"
                    var_unit = " per 1,000"
                elif 'tpr' in var_lower or 'positivity' in var_lower:
                    var_label = variable.replace('_', ' ').title()
                    var_unit = "%"
                elif 'population' in var_lower:
                    var_label = "Population"
                    var_unit = ""
                else:
                    var_label = variable.replace('_', ' ').title()
                    var_unit = ""

                for idx, row in df.iterrows():
                    # Ward name first (not LGA name!)
                    ward_name = row.get('WardName') or row.get('ward_name') or str(idx)
                    lga_name = row.get('LGAName') or row.get('lga_name') or 'Unknown'
                    val = row.get(variable)
                    lga_code = row.get('LGACode')

                    # Build hover text with clean structure
                    text = f"<b>Ward:</b> {ward_name}<br>"
                    text += f"<b>LGA:</b> {lga_name}<br>"

                    if pd.notna(val):
                        # Format value based on magnitude
                        if abs(val) >= 1000:
                            val_str = f"{val:,.0f}"
                        elif abs(val) >= 10:
                            val_str = f"{val:.1f}"
                        else:
                            val_str = f"{val:.2f}"

                        text += f"<br><b>{var_label}:</b> {val_str}{var_unit}"

                        if lga_code and lga_code in lga_averages:
                            lga_avg = lga_averages[lga_code]
                            diff = val - lga_avg
                            diff_sign = '+' if diff > 0 else ''
                            # Color: red if above average (higher burden = worse), green if below
                            diff_color = '#e74c3c' if diff > 0 else '#27ae60' if diff < 0 else '#666'

                            if abs(lga_avg) >= 1000:
                                avg_str = f"{lga_avg:,.0f}"
                            elif abs(lga_avg) >= 10:
                                avg_str = f"{lga_avg:.1f}"
                            else:
                                avg_str = f"{lga_avg:.2f}"

                            text += f"<br><b>LGA Average:</b> {avg_str}{var_unit}"
                            text += f" <span style='color:{diff_color}'>({diff_sign}{diff:.1f})</span>"
                    else:
                        text += f"<br><b>{var_label}:</b> No data"

                    texts.append(text)
                return texts

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
                    colorbar=dict(
                        title=variable.replace('_', ' ').title(),
                        thickness=15,
                        len=0.7
                    ) if show_scale else None,
                    name=name_suffix or variable
                ))

            if highlight_codes:
                faded = plot_data[~plot_data['_is_selected_lga']]
                highlighted = plot_data[plot_data['_is_selected_lga']]
                add_trace(
                    faded,
                    show_scale=False,
                    opacity=0.25,
                    colorscale_override=[[0, '#d1d5db'], [1, '#9ca3af']],
                    name_suffix='Other LGAs'
                )
                add_trace(highlighted, show_scale=True, opacity=0.85, name_suffix='Selected LGA')
            else:
                add_trace(plot_data, show_scale=True, opacity=0.75)

            # Add LGA boundary overlay for ward-level maps
            if plot_level == 'ward':
                add_lga_boundary_overlay(fig, clean_data)

            # Calculate map center
            bounds = plot_data.total_bounds
            center_lat = (bounds[1] + bounds[3]) / 2
            center_lon = (bounds[0] + bounds[2]) / 2
            
            # Update layout for professional map styling
            fig.update_layout(
                title={
                    'text': f'{variable.upper()} {title_suffix}',
                    'x': 0.5,
                    'xanchor': 'center',
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
            
            # Save map using the same pattern as other tools
            from datetime import datetime
            
            # Create unique filename with timestamp - ensures multiple visualizations coexist
            # Files persist until session closure (browser closed or session expired)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{variable}_distribution_map_{timestamp}.html"
            
            # Ensure session directory exists - use safe path access
            try:
                from flask import current_app
                upload_folder = current_app.config['UPLOAD_FOLDER']
            except RuntimeError:
                # Working outside of request context, use default path
                upload_folder = os.path.join('instance', 'uploads')
            
            session_dir = os.path.join(upload_folder, session_id)
            os.makedirs(session_dir, exist_ok=True)
            
            file_path = os.path.join(session_dir, filename)
            
            # Save the figure
            fig.write_html(file_path)
            
            # Generate web path for frontend
            web_path = f"/serve_viz_file/{session_id}/{filename}"

            # Inject LGA hover highlighting for ward-level maps
            if plot_level == 'ward':
                try:
                    # Extract LGA codes in the same order as the data
                    lga_codes = plot_data['LGACode'].fillna('').astype(str).tolist()
                    inject_lga_hover_highlight(file_path, lga_codes)
                except Exception as hover_err:
                    logger.warning(f"Failed to inject LGA hover highlight: {hover_err}")

            return {
                'type': 'spatial_distribution_map',
                'title': f'{variable.upper()} {title_suffix}',
                'file_path': file_path,  # Full path for backend processing
                'web_path': web_path,
                'description': f'Spatial distribution map of {variable} across study area',
            }
            
        except Exception as e:
            logger.error(f"Error creating spatial distribution map: {e}")
            return None
    
    def _generate_statistics(self, data: pd.DataFrame, variable: str) -> str:
        """Generate summary statistics text"""
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
            
            # Format based on variable type
            if variable.lower() in ['pfpr', 'tpr', 'u5_tpr_rdt']:
                # Percentage variables (already in percentage format, e.g., 60.38 = 60.38%)
                text = f"""**Summary Statistics:**
• **Records:** {stats['count']:,} wards ({stats['missing']} missing)
• **Average:** {stats['mean']:.2f}%
• **Range:** {stats['min']:.2f}% to {stats['max']:.2f}%
• **Standard deviation:** {stats['std']:.2f}
• **Median:** {stats['median']:.2f}%"""
            else:
                # Regular numeric variables
                text = f"""**Summary Statistics:**
• **Records:** {stats['count']:,} wards ({stats['missing']} missing)
• **Average:** {stats['mean']:.2f}
• **Range:** {stats['min']:.2f} to {stats['max']:.2f}
• **Standard deviation:** {stats['std']:.2f}
• **Median:** {stats['median']:.2f}"""
            
            return text
            
        except Exception as e:
            logger.error(f"Error generating statistics: {e}")
            return f"Statistics for {variable}: {len(data)} records"
    
    def _track_exploration_activity(self, session_id: str, variable_name: str):
        """Track user's exploration activity for workflow guidance"""
        try:
            try:
                from flask import session
                # Initialize exploration tracking if not exists
                if 'exploration_activity' not in session:
                    session['exploration_activity'] = {
                        'distributions_viewed': [],
                        'exploration_count': 0,
                        'started_at': None
                    }
                
                # Track this distribution view
                if variable_name not in session['exploration_activity']['distributions_viewed']:
                    session['exploration_activity']['distributions_viewed'].append(variable_name)
                    session['exploration_activity']['exploration_count'] += 1
                    
                    if session['exploration_activity']['started_at'] is None:
                        from datetime import datetime
                        session['exploration_activity']['started_at'] = datetime.now().isoformat()
                    
                    logger.info(f"📊 Tracked exploration: {variable_name} (count: {session['exploration_activity']['exploration_count']})")
            except RuntimeError:
                # Working outside of request context, skip session tracking
                logger.info(f"📊 Variable distribution created: {variable_name} (session tracking skipped - no Flask context)")
            
        except Exception as e:
            logger.warning(f"Failed to track exploration activity: {e}")
    
    def _generate_workflow_guidance(self, session_id: str) -> dict:
        """Generate intelligent workflow guidance based on exploration activity"""
        try:
            try:
                from flask import session
                exploration = session.get('exploration_activity', {})
                distributions_viewed = exploration.get('distributions_viewed', [])
                exploration_count = exploration.get('exploration_count', 0)
                
                # Check if comprehensive analysis has been run
                analysis_complete = session.get('comprehensive_analysis_complete', False)
            except RuntimeError:
                # Working outside of request context, provide minimal guidance
                exploration_count = 0
                distributions_viewed = []
                analysis_complete = False
            
            # Generate guidance based on activity
            if exploration_count == 0:
                return {
                    'show_guidance': False,
                    'message': '',
                    'phase': 'initial'
                }
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
            else:
                return {
                    'show_guidance': False,
                    'message': '',
                    'phase': 'unknown'
                }
                
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
