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
from pydantic import Field

from app.tools.base import BaseTool, ToolCategory, ToolExecutionResult
from app.services.variable_resolution_service import variable_resolver

logger = logging.getLogger(__name__)

class VariableDistribution(BaseTool):
    """Create spatial distribution maps for any variable from uploaded CSV and shapefile data"""
    
    variable_name: str = Field(..., description="Name of the variable to visualize (e.g., 'pfpr', 'rainfall', 'housing_quality')")
    
    @classmethod
    def get_tool_name(cls) -> str:
        return "variable_distribution"
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.VISUALIZATION
    
    @classmethod
    def get_description(cls) -> str:
        return "Create spatial distribution maps showing how any variable varies across the study area"
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show me the distribution of pfpr variable",
            "Create a map of housing_quality distribution", 
            "Visualize the spatial distribution of rainfall"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute variable distribution visualization"""
        try:
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
            
            # Create spatial distribution map
            map_result = self._create_spatial_distribution_map(csv_data, shapefile_data, resolved_variable, session_id)
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
                    'workflow_guidance': workflow_guidance
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
        """Load CSV and shapefile data from session"""
        try:
            upload_dir = os.path.join('instance', 'uploads', session_id)
            
            # Load raw CSV data
            csv_data = None
            csv_path = os.path.join(upload_dir, 'raw_data.csv')
            if os.path.exists(csv_path):
                csv_data = pd.read_csv(csv_path)
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
            
            return csv_data, shapefile_data
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None, None
    
    def _create_spatial_distribution_map(self, csv_data: pd.DataFrame, shapefile: gpd.GeoDataFrame, variable: str, session_id: str) -> Optional[Dict[str, Any]]:
        """Create spatial distribution choropleth map like vulnerability/composite score maps"""
        try:
            # Merge CSV data with shapefile
            join_columns = ['WardName', 'WardCode', 'LGACode', 'ward_name', 'ward_code']
            
            merged_data = None
            join_col = None
            
            for col in join_columns:
                if col in csv_data.columns and col in shapefile.columns:
                    try:
                        merged_data = shapefile.merge(csv_data, on=col, how='left')
                        join_col = col
                        logger.info(f"Successfully merged data on column: {col}")
                        break
                    except Exception as e:
                        logger.warning(f"Failed to merge on {col}: {e}")
                        continue
            
            if merged_data is None:
                logger.error("Could not merge CSV and shapefile data")
                return None
            
            # Ensure variable has valid data
            if variable not in merged_data.columns:
                logger.error(f"Variable {variable} not found in merged data")
                return None
            
            # Remove rows with missing values for the variable
            clean_data = merged_data.dropna(subset=[variable])
            if len(clean_data) == 0:
                logger.error(f"No valid data for variable {variable}")
                return None
            
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
            
            # Create choropleth map using plotly graph_objects (like other ChatMRPT maps)
            fig = go.Figure()
            
            # Add choropleth layer
            fig.add_trace(go.Choroplethmapbox(
                geojson=clean_data.geometry.__geo_interface__,
                locations=clean_data.index,
                z=clean_data[variable],
                colorscale=color_scale,
                text=clean_data.get('WardName', clean_data.get('ward_name', clean_data.index)),
                hovertemplate=f'<b>%{{text}}</b><br>{variable}: %{{z}}<extra></extra>',
                marker_opacity=0.7,
                marker_line_width=1,
                marker_line_color='white',
                showscale=True,
                colorbar=dict(
                    title=variable.replace('_', ' ').title(),
                    thickness=15,
                    len=0.7
                )
            ))
            
            # Calculate map center
            bounds = clean_data.total_bounds
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
            
            return {
                'type': 'spatial_distribution_map',
                'title': f'{variable.upper()} {title_suffix}',
                'file_path': filename,
                'web_path': web_path,
                'description': f'Spatial distribution map of {variable} across study area'
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
                # Percentage variables
                text = f"""**Summary Statistics:**
• **Records:** {stats['count']:,} wards ({stats['missing']} missing)
• **Average:** {stats['mean']:.3f} ({stats['mean']*100:.1f}%)
• **Range:** {stats['min']:.3f} to {stats['max']:.3f} ({stats['min']*100:.1f}% to {stats['max']*100:.1f}%)
• **Standard deviation:** {stats['std']:.3f}
• **Median:** {stats['median']:.3f} ({stats['median']*100:.1f}%)"""
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