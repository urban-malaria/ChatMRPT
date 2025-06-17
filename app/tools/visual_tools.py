"""
Visualization Tools for ChatMRPT - Dynamic Chart Generation

These tools create dynamic visualizations using Plotly and the unified dataset.
All charts are saved as HTML files and return web-accessible paths.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from flask import current_app
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime

logger = logging.getLogger(__name__)


def _get_unified_dataset(session_id: str) -> Optional[pd.DataFrame]:
    """Get the unified dataset for visualization."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        unified_gdf = load_unified_dataset(session_id)
        if unified_gdf is not None:
            return unified_gdf
        
        # Fallback to DataHandler
        data_service = current_app.services.data_service
        data_handler = data_service.get_handler(session_id)
        
        if data_handler and hasattr(data_handler, 'df') and data_handler.df is not None:
            return data_handler.df
        
        return None
        
    except Exception as e:
        logger.error(f"Error accessing dataset for visualization: {e}")
        return None


def _store_last_visualization(session_id: str, viz_result: Dict[str, Any]) -> None:
    """Store visualization result for explanation purposes."""
    try:
        # Store in session memory if available
        if not hasattr(current_app, 'session_memory'):
            current_app.session_memory = {}
        
        if session_id not in current_app.session_memory:
            current_app.session_memory[session_id] = {}
        
        current_app.session_memory[session_id]['last_visualization'] = viz_result
        logger.debug(f"Stored visualization for explanation: {viz_result.get('chart_type', 'unknown')}")
        
    except Exception as e:
        logger.error(f"Error storing visualization for explanation: {e}")


def _save_plotly_chart(fig, session_id: str, chart_name: str) -> Dict[str, str]:
    """Save Plotly chart with optimal display settings"""
    try:
        # Apply optimal layout settings for all charts
        fig.update_layout(
            # Responsive design
            autosize=True,
            responsive=True,
            
            # Optimal margins for better display
            margin=dict(l=60, r=60, t=80, b=60),
            
            # Enhanced interactivity
            hovermode='closest',
            dragmode='pan',
            
            # Better font settings
            font=dict(
                family="system-ui, -apple-system, sans-serif",
                size=12,
                color='#374151'
            ),
            
            # Modern styling
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            
            # Enhanced legend
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02,
                bgcolor="rgba(255,255,255,0.9)",
                bordercolor="rgba(0,0,0,0.1)",
                borderwidth=1
            ),
            
            # Better title styling
            title=dict(
                font=dict(size=16, color='#111827'),
                x=0.5,
                xanchor='center'
            )
        )
        
        # Configure modebar for better interactivity
        config = {
            "displayModeBar": True,
            "displaylogo": False,
            "modeBarButtonsToAdd": [
                "drawline",
                "drawopenpath",
                "drawclosedpath",
                "drawcircle",
                "drawrect",
                "eraseshape"
            ],
            "modeBarButtonsToRemove": ["lasso2d"],
            "toImageButtonOptions": {
                "format": "png",
                "filename": chart_name,
                "height": 800,
                "width": 1200,
                "scale": 2
            },
            "responsive": True,
            "scrollZoom": True
        }
        
        # Create session directory
        session_folder = f"instance/uploads/{session_id}"
        os.makedirs(session_folder, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{chart_name}_{timestamp}.html"
        file_path = os.path.join(session_folder, filename)
        
        # Save with enhanced configuration
        fig.write_html(
            file_path, 
            include_plotlyjs='cdn',
            config=config,
            div_id=f"plotly-div-{chart_name}",
            full_html=True
        )
        
        # Generate web path
        web_path = f"/serve_viz_file/{session_id}/{filename}"
        
        logger.info(f"💾 Saved enhanced interactive chart: {filename}")
        
        return {
            'file_path': file_path,
            'web_path': web_path,
            'filename': filename
        }
        
    except Exception as e:
        logger.error(f"Error saving chart: {e}")
        return {
            'file_path': None,
            'web_path': None,
            'filename': None
        }


def histogram(session_id: str, variable: str, bins: int = 30, color_by: str = None) -> Dict[str, Any]:
    """Create histogram visualization with optimal display settings."""
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable not in df.columns:
            return {'status': 'error', 'message': f'Variable {variable} not found'}
        
        # Create histogram with enhanced styling
        if color_by and color_by in df.columns:
            fig = px.histogram(df, x=variable, color=color_by, nbins=bins,
                             title=f'Distribution of {variable} by {color_by}',
                             marginal="box")
        else:
            fig = px.histogram(df, x=variable, nbins=bins,
                             title=f'Distribution of {variable}',
                             marginal="rug")
        
        # Apply optimal layout
        fig.update_layout(
            height=500,
            template='plotly_white',
            showlegend=True if color_by else False,
            xaxis_title=variable.replace('_', ' ').title(),
            yaxis_title='Count',
            bargap=0.1
        )
        
        # Enhanced hover information
        fig.update_traces(
            hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>'
        )
        
        paths = _save_plotly_chart(fig, session_id, f'histogram_{variable}')
        
        result = {
            'status': 'success',
            'message': f'Histogram created for {variable}',
            'chart_type': 'histogram',
            'variable': variable,
            'bins': bins,
            'color_by': color_by,
            **paths
        }
        
        # Store for explanation purposes
        _store_last_visualization(session_id, result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating histogram: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}


def boxplot(session_id: str, variable: str, group_by: str = None) -> Dict[str, Any]:
    """Create boxplot visualization with optimal display settings."""
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable not in df.columns:
            return {'status': 'error', 'message': f'Variable {variable} not found'}
        
        # Create boxplot with enhanced styling
        if group_by and group_by in df.columns:
            fig = px.box(df, y=variable, x=group_by, 
                        title=f'{variable} Distribution by {group_by}',
                        points="outliers")
        else:
            fig = px.box(df, y=variable, 
                        title=f'{variable} Distribution',
                        points="outliers")
        
        # Apply optimal layout
        fig.update_layout(
            height=500,
            template='plotly_white',
            xaxis_title=group_by.replace('_', ' ').title() if group_by else '',
            yaxis_title=variable.replace('_', ' ').title(),
            showlegend=False
        )
        
        # Enhanced hover and styling
        fig.update_traces(
            boxpoints='outliers',
            jitter=0.3,
            pointpos=-1.8,
            hovertemplate='<b>%{y}</b><extra></extra>'
        )
        
        paths = _save_plotly_chart(fig, session_id, f'boxplot_{variable}')
        
        result = {
            'status': 'success',
            'message': f'Boxplot created for {variable}',
            'chart_type': 'boxplot',
            'variable': variable,
            'group_by': group_by,
            **paths
        }
        
        # Store for explanation purposes
        _store_last_visualization(session_id, result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating boxplot: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}


def bar_chart(session_id: str, x_variable: str, y_variable: str = None, color_by: str = None) -> Dict[str, Any]:
    """Create bar chart visualization."""
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if x_variable not in df.columns:
            return {'status': 'error', 'message': f'Variable {x_variable} not found'}
        
        # Create bar chart
        if y_variable and y_variable in df.columns:
            if color_by and color_by in df.columns:
                fig = px.bar(df, x=x_variable, y=y_variable, color=color_by,
                           title=f'{y_variable} by {x_variable}')
            else:
                fig = px.bar(df, x=x_variable, y=y_variable,
                           title=f'{y_variable} by {x_variable}')
        else:
            # Count plot
            if color_by and color_by in df.columns:
                fig = px.histogram(df, x=x_variable, color=color_by,
                                 title=f'Count of {x_variable}')
            else:
                fig = px.histogram(df, x=x_variable, title=f'Count of {x_variable}')
        
        fig.update_layout(height=500, template='plotly_white')
        chart_name = f'bar_chart_{x_variable}_{y_variable}' if y_variable else f'bar_chart_{x_variable}'
        paths = _save_plotly_chart(fig, session_id, chart_name)
        
        result = {
            'status': 'success',
            'message': f'Bar chart created for {x_variable}',
            'chart_type': 'bar_chart',
            'x_variable': x_variable,
            'y_variable': y_variable,
            'color_by': color_by,
            **paths
        }
        
        # Store for explanation purposes
        _store_last_visualization(session_id, result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating bar chart: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}


def line_chart(session_id: str, x_variable: str, y_variable: str, color_by: str = None) -> Dict[str, Any]:
    """Create line chart visualization."""
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if x_variable not in df.columns or y_variable not in df.columns:
            return {'status': 'error', 'message': 'Variables not found in dataset'}
        
        # Sort by x variable
        df_sorted = df.sort_values(x_variable)
        
        # Create line chart
        if color_by and color_by in df.columns:
            fig = px.line(df_sorted, x=x_variable, y=y_variable, color=color_by,
                         title=f'{y_variable} vs {x_variable}')
        else:
            fig = px.line(df_sorted, x=x_variable, y=y_variable,
                         title=f'{y_variable} vs {x_variable}')
        
        fig.update_layout(height=500, template='plotly_white')
        paths = _save_plotly_chart(fig, session_id, f'line_chart_{x_variable}_{y_variable}')
        
        result = {
            'status': 'success',
            'message': f'Line chart created for {y_variable} vs {x_variable}',
            'chart_type': 'line_chart',
            'x_variable': x_variable,
            'y_variable': y_variable,
            'color_by': color_by,
            **paths
        }
        
        # Store for explanation purposes
        _store_last_visualization(session_id, result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating line chart: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}


def scatter_plot(session_id: str, x_variable: str, y_variable: str, color_by: str = None, size_by: str = None) -> Dict[str, Any]:
    """Create scatter plot visualization with optimal display settings."""
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        missing_vars = [var for var in [x_variable, y_variable] if var not in df.columns]
        if missing_vars:
            return {'status': 'error', 'message': f'Variables not found: {missing_vars}'}
        
        # Create scatter plot with enhanced styling
        fig = px.scatter(
            df, x=x_variable, y=y_variable,
            color=color_by if color_by and color_by in df.columns else None,
            size=size_by if size_by and size_by in df.columns else None,
            title=f'{y_variable} vs {x_variable}',
            trendline="ols",
            marginal_x="histogram",
            marginal_y="histogram"
        )
        
        # Apply optimal layout
        fig.update_layout(
            height=600,
            template='plotly_white',
            xaxis_title=x_variable.replace('_', ' ').title(),
            yaxis_title=y_variable.replace('_', ' ').title(),
            showlegend=True if color_by else False
        )
        
        # Enhanced hover information
        hover_template = f'<b>{x_variable}</b>: %{{x}}<br><b>{y_variable}</b>: %{{y}}'
        if color_by:
            hover_template += f'<br><b>{color_by}</b>: %{{marker.color}}'
        hover_template += '<extra></extra>'
        
        fig.update_traces(hovertemplate=hover_template)
        
        paths = _save_plotly_chart(fig, session_id, f'scatter_{x_variable}_{y_variable}')
        
        result = {
            'status': 'success',
            'message': f'Scatter plot created: {y_variable} vs {x_variable}',
            'chart_type': 'scatter_plot',
            'x_variable': x_variable,
            'y_variable': y_variable,
            'color_by': color_by,
            'size_by': size_by,
            **paths
        }
        
        # Store for explanation purposes
        _store_last_visualization(session_id, result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating scatter plot: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}


def heatmap(session_id: str, variables: List[str] = None, method: str = 'pearson') -> Dict[str, Any]:
    """Create correlation heatmap with optimal display settings."""
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        # Get numeric variables
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        available_vars = variables if variables else numeric_cols
        available_vars = [var for var in available_vars if var in numeric_cols]
        
        if len(available_vars) < 2:
            return {'status': 'error', 'message': 'Need at least 2 numeric variables for correlation heatmap'}
        
        # Calculate correlation matrix
        corr_matrix = df[available_vars].corr(method=method)
        
        # Create enhanced heatmap
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.columns,
            colorscale='RdBu',
            zmid=0,
            text=np.round(corr_matrix.values, 2),
            texttemplate="%{text}",
            textfont={"size": 10},
            hoverongaps=False,
            hovertemplate='<b>%{x}</b> vs <b>%{y}</b><br>Correlation: %{z:.3f}<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'Correlation Heatmap ({method.title()} method)',
            height=max(600, len(available_vars) * 40),
            width=max(600, len(available_vars) * 40),
            template='plotly_white',
            xaxis=dict(side='bottom'),
            yaxis=dict(side='left')
        )
        
        paths = _save_plotly_chart(fig, session_id, f'heatmap_correlation_{method}')
        
        result = {
            'status': 'success',
            'message': f'Correlation heatmap created using {method} method',
            'chart_type': 'heatmap',
            'variables': available_vars,
            'method': method,
            **paths
        }
        
        # Store for explanation purposes
        _store_last_visualization(session_id, result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating heatmap: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}


def pie_chart(session_id: str, variable: str, limit: int = 10) -> Dict[str, Any]:
    """Create pie chart visualization."""
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable not in df.columns:
            return {'status': 'error', 'message': f'Variable {variable} not found'}
        
        # Get value counts
        value_counts = df[variable].value_counts().head(limit)
        
        if len(value_counts) == 0:
            return {'status': 'error', 'message': f'No data available for {variable}'}
        
        # Create pie chart
        fig = px.pie(values=value_counts.values, names=value_counts.index,
                    title=f'Distribution of {variable}')
        
        fig.update_layout(height=500, template='plotly_white')
        paths = _save_plotly_chart(fig, session_id, f'pie_chart_{variable}')
        
        result = {
            'status': 'success',
            'message': f'Pie chart created for {variable}',
            'chart_type': 'pie_chart',
            'variable': variable,
            'limit': limit,
            **paths
        }
        
        # Store for explanation purposes
        _store_last_visualization(session_id, result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating pie chart: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}


def map_plot(session_id: str, color_variable: str = None, size_variable: str = None) -> Dict[str, Any]:
    """Create geographic map visualization with optimal display settings."""
    try:
        df = _get_unified_dataset(session_id)
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        # Check for geographic data
        lat_cols = [col for col in df.columns if 'lat' in col.lower()]
        lon_cols = [col for col in df.columns if 'lon' in col.lower()]
        
        if not (lat_cols and lon_cols):
            return {'status': 'error', 'message': 'No geographic data (lat/lon) found in dataset'}
        
        lat_col, lon_col = lat_cols[0], lon_cols[0]
        
        # Create enhanced scatter mapbox
        fig = px.scatter_map(
            df, lat=lat_col, lon=lon_col,
            color=color_variable if color_variable and color_variable in df.columns else None,
            size=size_variable if size_variable and size_variable in df.columns else None,
            title='Geographic Distribution',
            hover_data=[col for col in df.columns if col not in [lat_col, lon_col]][:5],
            zoom=10
        )
        
        # Apply optimal map layout
        fig.update_layout(
            height=700,
            template='plotly_white',
            map=dict(
                style='open-street-map',
                center=dict(
                    lat=df[lat_col].mean(),
                    lon=df[lon_col].mean()
                ),
                zoom=10
            ),
            margin=dict(l=0, r=0, t=60, b=0)
        )
        
        # Enhanced hover information
        fig.update_traces(
            hovertemplate='<b>Location</b><br>Lat: %{lat}<br>Lon: %{lon}<extra></extra>'
        )
        
        paths = _save_plotly_chart(fig, session_id, f'map_plot_{color_variable or "basic"}')
        
        result = {
            'status': 'success',
            'message': 'Geographic map created',
            'chart_type': 'map_plot',
            'color_variable': color_variable,
            'size_variable': size_variable,
            **paths
        }
        
        # Store for explanation purposes
        _store_last_visualization(session_id, result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating map plot: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}


# Alias for compatibility with test expectations
box_plot = boxplot

# Alias for compatibility
box_plot = boxplot 

# Flexible wrapper functions for better parameter handling
def box_plot_flexible(session_id: str, variable: str = None, group_by: str = None) -> Dict[str, Any]:
    """Flexible box plot wrapper that provides intelligent defaults"""
    try:
        # If no variable specified, try to find a reasonable default
        if variable is None:
            df = _get_unified_dataset(session_id)
            if df is None:
                return {'status': 'error', 'message': 'No dataset available'}
            
            # Look for common analysis variables in order of preference
            candidate_vars = ['composite_score', 'composite_rank', 'pca_score', 'pca_rank', 'overall_rank']
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            # Try candidates first
            for var in candidate_vars:
                if var in df.columns:
                    variable = var
                    break
            
            # If no candidates found, use first numeric column
            if variable is None and numeric_cols:
                variable = numeric_cols[0]
            
            if variable is None:
                return {'status': 'error', 'message': 'No numeric variables found for box plot'}
        
        # Call the original boxplot function
        return boxplot(session_id, variable, group_by)
        
    except Exception as e:
        logger.error(f"Error in flexible box plot: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}


def scatter_plot_flexible(session_id: str, variable1: str = None, variable2: str = None, 
                         x_variable: str = None, y_variable: str = None, 
                         color_by: str = None, size_by: str = None) -> Dict[str, Any]:
    """Flexible scatter plot wrapper that handles different parameter names"""
    try:
        # Handle parameter aliases
        if x_variable is None and variable1:
            x_variable = variable1
        if y_variable is None and variable2:
            y_variable = variable2
            
        # If still no variables, try to find reasonable defaults
        if x_variable is None or y_variable is None:
            df = _get_unified_dataset(session_id)
            if df is None:
                return {'status': 'error', 'message': 'No dataset available'}
            
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            if len(numeric_cols) < 2:
                return {'status': 'error', 'message': 'Need at least 2 numeric variables for scatter plot'}
            
            if x_variable is None:
                x_variable = numeric_cols[0]
            if y_variable is None:
                # Try to find a different variable than x_variable
                for col in numeric_cols:
                    if col != x_variable:
                        y_variable = col
                        break
                if y_variable is None:  # fallback
                    y_variable = numeric_cols[1] if len(numeric_cols) > 1 else numeric_cols[0]
        
        # Call the original scatter_plot function
        return scatter_plot(session_id, x_variable, y_variable, color_by, size_by)
        
    except Exception as e:
        logger.error(f"Error in flexible scatter plot: {e}")
        return {'status': 'error', 'message': f'Error: {str(e)}'}