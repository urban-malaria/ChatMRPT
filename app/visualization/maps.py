# app/visualization/maps.py
import os
import json
import logging
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, List, Optional, Any, Union
import uuid
import geopandas as gpd
from flask import current_app

# Import from other visualization modules
from .core import get_full_variable_name, get_variable_by_name, is_id_column
from .export import ensure_wgs84_crs, prepare_geodataframe_for_json, create_plotly_html
from .themes import get_map_styling, get_color_scheme, get_risk_labels, apply_theme_to_figure, get_chart_styling

# Set up logging
logger = logging.getLogger(__name__)


def create_variable_map(data_handler, variable_name=None):
    """
    Create a map visualizing a variable's distribution
    
    Args:
        data_handler: DataHandler instance
        variable_name: Name of the variable to visualize
        
    Returns:
        dict: Status and visualization information
    """
    try:
        # Check if shapefile data is available
        if data_handler.shapefile_data is None:
            return {
                'status': 'error',
                'message': 'Shapefile data not loaded'
            }
        
        # If no variable specified, pick the first suitable variable
        if variable_name is None:
            if data_handler.csv_data is not None:
                var_columns = [col for col in data_handler.csv_data.columns 
                            if col != 'WardName' and not is_id_column(col)]
                if var_columns:
                    variable_name = var_columns[0]
        
        # Find the best matching variable - IMPORTANT: Always allow access to all variables in CSV data
        actual_variable = get_variable_by_name(data_handler, variable_name)
        
        if not actual_variable:
            available_vars = []
            if data_handler.csv_data is not None:
                available_vars = [col for col in data_handler.csv_data.columns 
                               if col != 'WardName' and pd.api.types.is_numeric_dtype(data_handler.csv_data[col]) 
                               and not is_id_column(col)]
            
            return {
                'status': 'error',
                'message': 'Variable similar to "{}" not found in data'.format(variable_name),
                'available_variables': available_vars
            }
        
        # Check if this variable has missing values that were cleaned
        has_missing = False
        missing_count = 0
        
        # Use csv_data for original values
        df = data_handler.csv_data
        
        if actual_variable in df.columns:
            missing_count = df[actual_variable].isna().sum()
            has_missing = missing_count > 0
        
        # Get full variable name for display
        full_variable_name = get_full_variable_name(actual_variable)
        
        # Get a copy of the shapefile with standardized CRS
        shapefile_data = ensure_wgs84_crs(data_handler.shapefile_data)
        
        # If we have missing values and cleaned data, show both maps
        if has_missing and data_handler.cleaned_data is not None:
            # Create figure with two subplots side by side
            fig = make_subplots(
                rows=1, cols=2,
                specs=[[{"type": "mapbox"}, {"type": "mapbox"}]],
                subplot_titles=["Original Data ({} missing values)".format(missing_count), "Cleaned Data"],
                horizontal_spacing=0.02
            )
            
            # 1. Original data map
            # Create combined dataframe for plotting
            gdf_original = shapefile_data.merge(df[['WardName', actual_variable]], on='WardName', how='left')
            
            # Convert geometry to geojson with proper serialization
            gdf_prepared = prepare_geodataframe_for_json(gdf_original)
            geojson = json.loads(gdf_prepared.to_json())
            
            # Add choropleth for original data
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=geojson,
                    locations=gdf_original.index,
                    z=gdf_original[actual_variable],
                    colorscale='Blues',
                    marker_opacity=0.8,
                    marker_line_width=0.5,
                    marker_line_color='black',
                    hovertemplate='<b>%{customdata}</b><br>' + '{}: '.format(full_variable_name) + '%{z:.2f}<extra></extra>',
                    customdata=gdf_original['WardName'],
                    showscale=False
                ),
                row=1, col=1
            )
            
            # 2. Cleaned data map
            # Create combined dataframe for plotting
            gdf_cleaned = shapefile_data.merge(data_handler.cleaned_data[['WardName', actual_variable]], on='WardName', how='left')
            
            # Convert geometry to geojson with proper serialization
            gdf_prepared = prepare_geodataframe_for_json(gdf_cleaned)
            geojson = json.loads(gdf_prepared.to_json())
            
            # Add choropleth for cleaned data
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=geojson,
                    locations=gdf_cleaned.index,
                    z=gdf_cleaned[actual_variable],
                    colorscale='Blues',
                    marker_opacity=0.8,
                    marker_line_width=0.5,
                    marker_line_color='black',
                    hovertemplate='<b>%{customdata}</b><br>' + '{}: '.format(full_variable_name) + '%{z:.2f}<extra></extra>',
                    customdata=gdf_cleaned['WardName'],
                    colorbar=dict(
                        title=dict(
                            text=full_variable_name,
                            font=dict(size=12)
                        )
                    )
                ),
                row=1, col=2
            )
            
            # Get proper map centering
            center_lat = gdf_original.geometry.centroid.y.mean()
            center_lon = gdf_original.geometry.centroid.x.mean()
            
            # Calculate appropriate zoom level based on the bounding box
            bounds = gdf_original.geometry.total_bounds  # minx, miny, maxx, maxy
            span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
            span_y = max(0.01, bounds[3] - bounds[1])
            
            # Calculate zoom level - ensure it's reasonable
            zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
            
            # Update mapbox settings for both subplots
            fig.update_mapboxes(
                style="carto-positron",
                center={"lat": center_lat, "lon": center_lon},
                zoom=zoom_level
            )
        
        else:
            # Single map - just show the data we have
            fig = go.Figure()
            
            # Use cleaned data if available and the variable exists there, otherwise use original
            if data_handler.cleaned_data is not None and actual_variable in data_handler.cleaned_data.columns:
                df_to_use = data_handler.cleaned_data
            else:
                df_to_use = df
            
            # Create combined dataframe for plotting
            gdf = shapefile_data.merge(df_to_use[['WardName', actual_variable]], on='WardName', how='left')
            
            # Convert geometry to geojson with proper serialization
            gdf_prepared = prepare_geodataframe_for_json(gdf)
            geojson = json.loads(gdf_prepared.to_json())
            
            # Get proper map centering
            center_lat = gdf.geometry.centroid.y.mean()
            center_lon = gdf.geometry.centroid.x.mean()
            
            # Calculate appropriate zoom level based on the bounding box
            bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
            span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
            span_y = max(0.01, bounds[3] - bounds[1])
            
            # Calculate zoom level - ensure it's reasonable
            zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
            
            # Add choropleth
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=geojson,
                    locations=gdf.index,
                    z=gdf[actual_variable],
                    colorscale='Blues',
                    marker_opacity=0.8,
                    marker_line_width=0.5,
                    marker_line_color='black',
                    hovertemplate='<b>%{customdata}</b><br>' + '{}: '.format(full_variable_name) + '%{z:.2f}<extra></extra>',
                    customdata=gdf['WardName'],
                    colorbar=dict(
                        title=dict(
                            text=full_variable_name,
                            font=dict(size=12)
                        )
                    )
                )
            )
            
            # Update mapbox settings
            fig.update_layout(
                mapbox=dict(
                    style="carto-positron",
                    center={"lat": center_lat, "lon": center_lon},
                    zoom=zoom_level
                )
            )
        
        # Update overall layout - IMPORTANT CHANGES HERE
        fig.update_layout(
            title={
                'text': "Distribution of {}".format(full_variable_name),
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 20}
            },
            # Remove fixed dimensions
            # height=480,  # Remove fixed height
            # width=800,   # Remove fixed width
            margin=dict(l=20, r=20, t=80, b=20),
            autosize=True,  # Enable responsive sizing
            paper_bgcolor='rgba(255,255,255,0.8)',
            plot_bgcolor='rgba(255,255,255,0.8)'
        )
        
        # Create HTML file with improved config
        html_path = create_plotly_html(
            fig, 
            "variable_map_{}.html".format(actual_variable), 
            config={
                'responsive': True,
                'displayModeBar': True,
                'scrollZoom': True,
                'fillFrame': True  # Fill frame completely
            }
        )
        
        # Prepare data summary for LLM context
        if df is not None and actual_variable in df.columns:
            values = df[actual_variable].dropna().values
            data_stats = {
                'min': float(np.min(values)) if len(values) > 0 else None,
                'max': float(np.max(values)) if len(values) > 0 else None,
                'mean': float(np.mean(values)) if len(values) > 0 else None,
                'median': float(np.median(values)) if len(values) > 0 else None,
                'missing_count': int(missing_count),
                'missing_percentage': float(missing_count / len(df) * 100) if len(df) > 0 else 0
            }
        else:
            data_stats = {'error': 'Statistics not available'}

        # Create rich context for LLM
        data_summary = {
            'variable': actual_variable,
            'full_variable_name': full_variable_name,
            'missing_values': missing_count,
            'has_missing_values': has_missing,
            'cleaned_data_available': data_handler.cleaned_data is not None,
            'statistics': data_stats
        }
        
        visual_elements = {
            'map_type': 'choropleth',
            'color_scale': 'Blues',
            'color_meaning': 'darker blue = higher values',
            'split_view': has_missing and data_handler.cleaned_data is not None
        }

        # Return success with paths and metadata
        return {
           'status': 'success',
           'message': 'Successfully created map for {}'.format(full_variable_name),
           'image_path': html_path,
           'variable': actual_variable,
           'full_variable_name': full_variable_name,
           'missing_values': missing_count,
           'viz_type': 'variable_map',
           'data_summary': data_summary,
           'visual_elements': visual_elements
        }
       
    except Exception as e:
       logger.error("Error creating variable map: {}".format(str(e)), exc_info=True)
       import traceback
       logger.error(traceback.format_exc())
       return {
           'status': 'error',
           'message': 'Error creating variable map: {}'.format(str(e))
       }


def create_normalized_map(data_handler, variable_name=None):
    """
    Create a map visualizing a normalized variable
    
    Args:
        data_handler: DataHandler instance
        variable_name: Name of the variable to visualize
        
    Returns:
        dict: Status and visualization information
    """
    try:
        # If no variable specified, pick the first suitable variable
        if variable_name is None:
            if data_handler.normalized_data is not None:
                norm_cols = [col for col in data_handler.normalized_data.columns 
                            if col.startswith('normalization_')]
                if norm_cols:
                    variable_name = norm_cols[0].replace('normalization_', '')
            elif data_handler.cleaned_data is not None:
                var_columns = [col for col in data_handler.cleaned_data.columns 
                            if col != 'WardName' and not is_id_column(col)]
                if var_columns:
                    variable_name = var_columns[0]
            elif data_handler.csv_data is not None:
                var_columns = [col for col in data_handler.csv_data.columns 
                            if col != 'WardName' and not is_id_column(col)]
                if var_columns:
                    variable_name = var_columns[0]
        
        # Check if normalized data is available
        if data_handler.normalized_data is None:
            return {
                'status': 'error',
                'message': 'Normalized data not available. Run analysis first.'
            }
        
        if data_handler.shapefile_data is None:
            return {
                'status': 'error',
                'message': 'Shapefile data not loaded'
            }
        
        # Find the best matching variable
        actual_variable = get_variable_by_name(data_handler, variable_name)
        
        if not actual_variable:
            # Try to check available normalized columns
            norm_vars = []
            if data_handler.normalized_data is not None:
                norm_vars = [col.replace('normalization_', '') for col in data_handler.normalized_data.columns 
                           if col.startswith('normalization_')]
            
            # If no match but we do have normalized variables, use the first one
            if norm_vars:
                actual_variable = norm_vars[0]
            else:
                available_vars = []
                if data_handler.csv_data is not None:
                    available_vars = [col for col in data_handler.csv_data.columns 
                                   if col != 'WardName' and pd.api.types.is_numeric_dtype(data_handler.csv_data[col]) 
                                   and not is_id_column(col)]
                
                return {
                    'status': 'error',
                    'message': 'Variable similar to "{}" not found and no normalized variables available'.format(variable_name),
                    'available_variables': available_vars
                }
        
        # Normalized column name
        norm_col = "normalization_{}".format(actual_variable.lower())
        
        # Check if the normalized column exists
        if norm_col not in data_handler.normalized_data.columns:
            # Try to find a similar normalized column
            all_norm_cols = [col for col in data_handler.normalized_data.columns if col.startswith('normalization_')]
            
            # Try direct match with variable name (case insensitive)
            similar_cols = [col for col in all_norm_cols 
                          if actual_variable.lower() == col.replace('normalization_', '').lower()]
            
            # If no direct match, try partial match
            if not similar_cols:
                similar_cols = [col for col in all_norm_cols 
                              if actual_variable.lower() in col.replace('normalization_', '').lower()]
            
            if similar_cols:
                norm_col = similar_cols[0]
                # Extract original variable name from normalized column name
                actual_variable = norm_col.replace('normalization_', '')
                logger.info("Found normalized column '{}' for variable '{}'".format(norm_col, variable_name))
            else:
                return {
                    'status': 'error',
                    'message': 'Normalized column for variable {} not found'.format(actual_variable)
                }
        
        # Get variable relationship
        relationship = 'direct'
        if hasattr(data_handler, 'variable_relationships') and actual_variable in data_handler.variable_relationships:
            relationship = data_handler.variable_relationships[actual_variable]
        
        # Get full variable name for display
        full_variable_name = get_full_variable_name(actual_variable)
        
        # Get a copy of the shapefile with standardized CRS
        shapefile_data = ensure_wgs84_crs(data_handler.shapefile_data)
        
        # Create combined dataframe for plotting
        gdf = shapefile_data.merge(
            data_handler.normalized_data[['WardName', norm_col]], 
            on='WardName', 
            how='left'
        )
        
        # Convert geometry to geojson with proper serialization
        gdf_prepared = prepare_geodataframe_for_json(gdf)
        geojson = json.loads(gdf_prepared.to_json())
        
        # Get proper map centering
        center_lat = gdf.geometry.centroid.y.mean()
        center_lon = gdf.geometry.centroid.x.mean()
        
        # Calculate appropriate zoom level based on the bounding box
        bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
        span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
        span_y = max(0.01, bounds[3] - bounds[1])
        
        # Calculate zoom level - ensure it's reasonable
        zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
        
        # Create choropleth map with Plotly
        fig = go.Figure()
        
        fig.add_trace(go.Choroplethmapbox(
            geojson=geojson,
            locations=gdf.index,
            z=gdf[norm_col],
            colorscale='YlOrRd',
            marker_opacity=0.8,
            marker_line_width=0.5,
            marker_line_color='black',
            hovertemplate='<b>%{customdata}</b><br>Normalized Value: %{z:.3f}<extra></extra>',
            customdata=gdf['WardName'],
            zmin=0,
            zmax=1,
            colorbar=dict(
                title=dict(
                    text='Risk Contribution' if relationship == 'direct' else 'Risk Contribution (Inverted)',
                    font=dict(size=12)
                ),
                tickvals=[0, 0.25, 0.5, 0.75, 1],
                ticktext=['Very Low', 'Low', 'Medium', 'High', 'Very High']
            )
        ))
        
        # Update layout
        fig.update_layout(
            title={
                'text': "Normalized {} ({} relationship)".format(full_variable_name, relationship),
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 20}
            },
            mapbox=dict(
                style="carto-positron",
                center={"lat": center_lat, "lon": center_lon},
                zoom=zoom_level
            ),
            margin=dict(l=20, r=20, t=80, b=20),
            autosize=True
        )
        
        # Create HTML file
        html_path = create_plotly_html(fig, "normalized_map_{}.html".format(actual_variable))
        
        # Prepare data summary for LLM context
        norm_values = gdf[norm_col].dropna().values
        data_stats = {
            'min': float(np.min(norm_values)) if len(norm_values) > 0 else None,
            'max': float(np.max(norm_values)) if len(norm_values) > 0 else None,
            'mean': float(np.mean(norm_values)) if len(norm_values) > 0 else None,
            'median': float(np.median(norm_values)) if len(norm_values) > 0 else None
        }
        
        # Create rich context for LLM
        data_summary = {
            'variable': actual_variable,
            'full_variable_name': full_variable_name,
            'relationship': relationship,
            'relationship_explanation': "Higher values correspond to higher malaria risk" if relationship == "direct" else 
                                      "Higher values correspond to lower malaria risk (the relationship is inverted)",
            'statistics': data_stats
        }
        
        visual_elements = {
            'map_type': 'choropleth',
            'color_scale': 'YlOrRd',
            'color_meaning': 'yellow to dark red (low to high risk contribution)',
            'scale_range': '0-1 normalized values'
        }
        
        # Return success with paths and metadata
        return {
            'status': 'success',
            'message': 'Successfully created normalized map for {}'.format(full_variable_name),
            'image_path': html_path,
            'variable': actual_variable,
            'full_variable_name': full_variable_name,
            'relationship': relationship,
            'viz_type': 'normalized_map',
            'data_summary': data_summary,
            'visual_elements': visual_elements
        }
        
    except Exception as e:
        logger.error("Error creating normalized map: {}".format(str(e)), exc_info=True)
        import traceback
        logger.error(traceback.format_exc())
        return {
            'status': 'error',
            'message': 'Error creating normalized map: {}'.format(str(e))
        }


def create_composite_map(data_handler, model_index=None):
    """
    Create composite risk score maps
    
    Args:
        data_handler: DataHandler instance
        model_index: Index of the model/page to visualize (None for first page)
        
    Returns:
        dict: Status and visualization information
    """
    try:
        # Check if composite scores are available
        if not hasattr(data_handler, 'composite_scores') or data_handler.composite_scores is None:
            return {
                'status': 'error',
                'message': 'Composite scores not available. Calculate composite scores first.'
            }
        
        if data_handler.shapefile_data is None:
            return {
                'status': 'error',
                'message': 'Shapefile data not loaded'
            }
        
        # Get all model columns
        model_columns = [col for col in data_handler.composite_scores['scores'].columns if col.startswith('model_')]
        model_formulas = data_handler.composite_scores['formulas']
        
        # Determine number of models and pages
        n_models = len(model_columns)
        models_per_page = 4
        n_pages = (n_models + models_per_page - 1) // models_per_page
        
        # If model_index is a number, treat it as a page number
        page = 1
        if isinstance(model_index, int) or isinstance(model_index, float) or (isinstance(model_index, str) and model_index.isdigit()):
            page = int(model_index)
            # Ensure page is within bounds
            page = max(1, min(page, n_pages))
        
        # Calculate start and end indices for this page
        start_idx = (page - 1) * models_per_page
        end_idx = min(start_idx + models_per_page, n_models)
        
        # Get models for this page
        page_models = model_columns[start_idx:end_idx]
        page_formulas = model_formulas[start_idx:end_idx]
        
        # Get a copy of the shapefile with standardized CRS
        shapefile_data = ensure_wgs84_crs(data_handler.shapefile_data)
        
        # Check if the shapefile has an Urban column to identify non-urban wards
        urban_column = None
        for col in ['Urban', 'urban', 'URBAN', 'UrbanStatus']:
            if col in shapefile_data.columns:
                urban_column = col
                break
        
        # Combine with shapefile
        gdf = shapefile_data.merge(
            data_handler.composite_scores['scores'],
            on='WardName',
            how='left'
        )
        
        # If we have an Urban column, identify "Not Ideal" models
        not_ideal_models = {}
        if urban_column is not None:
            # For each model, check if non-urban wards (Urban="No") are in the top 5 for vulnerability
            for model in model_columns:
                # Sort wards by model score (descending) to find top 5
                top_wards = gdf.sort_values(model, ascending=False).head(5)
                
                # Check if any of these wards are non-urban
                non_urban_top_wards = top_wards[top_wards[urban_column].str.lower().isin(['no', 'false', '0', 'n'])]
                
                # If there are non-urban wards in top 5, flag this model as "Not Ideal"
                if len(non_urban_top_wards) > 0:
                    not_ideal_models[model] = non_urban_top_wards['WardName'].tolist()
        
        # Convert geometry to geojson with proper serialization
        gdf_prepared = prepare_geodataframe_for_json(gdf)
        geojson = json.loads(gdf_prepared.to_json())
        
        # Get proper map centering
        center_lat = gdf.geometry.centroid.y.mean()
        center_lon = gdf.geometry.centroid.x.mean()
        
        # Calculate appropriate zoom level based on the bounding box
        bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
        span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
        span_y = max(0.01, bounds[3] - bounds[1])
        
        # Calculate zoom level - ensure it's reasonable
        zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
        
        # Determine grid layout for subplots
        if len(page_models) == 1:
            rows, cols = 1, 1
        elif len(page_models) == 2:
            rows, cols = 1, 2
        else:
            rows, cols = 2, 2
        
        # Create subplot titles with variables on separate lines
        subplot_titles = []
        for model, formula in zip(page_models, page_formulas):
            # Get variables
            variables = formula['variables']
            
            # Check if we have any variables
            if variables and len(variables) > 0:
                # Create title with variables on separate lines
                var_names = []
                for var in variables:
                    # Get full name if available
                    var_name = get_full_variable_name(var.lower())
                    var_names.append(var_name)
                
                # Join with line breaks
                title = "<br>".join(var_names)
                
                # Add "Not Ideal" designation if this model is flagged
                if model in not_ideal_models:
                    title = "{}<br><span class='not-ideal-label'>(Not Ideal)</span>".format(title)
            else:
                # Fallback if no variables
                title = "{}".format(model.replace('model_', 'Model '))
                if model in not_ideal_models:
                    title = "{}<br><span class='not-ideal-label'>(Not Ideal)</span>".format(title)
                
            subplot_titles.append(title)
        
        # Create subplots
        fig = make_subplots(
            rows=rows,
            cols=cols,
            specs=[[{"type": "mapbox"}] * cols for _ in range(rows)],
            subplot_titles=subplot_titles,
            vertical_spacing=0.22,  # Increased vertical spacing significantly
            horizontal_spacing=0.05 # Can adjust this too if needed
        )
        
        # Get styling from theme module
        map_styling = get_map_styling()
        colorscale = get_color_scheme('composite_map')
        risk_labels = get_risk_labels()
        chart_styling = get_chart_styling()
        
        # Add choropleth for each model
        for idx, model in enumerate(page_models):
            row = idx // cols + 1
            col = idx % cols + 1
            
            # Add choropleth trace for the model
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=geojson,
                    locations=gdf.index,
                    z=gdf[model],
                    colorscale=colorscale,  # Use theme colorscale
                    marker_line_color=map_styling['marker_line_color'],
                    marker_line_width=map_styling['marker_line_width'],
                    showscale=(idx == 0),  # Only show scale for first plot
                    colorbar=dict(
                        title=dict(
                            text="Risk Score",
                            font=dict(size=12)
                        ),
                        tickvals=risk_labels['tickvals'],  # Five tick values from theme
                        ticktext=risk_labels['ticktext']   # Five labels from theme
                    ) if idx == 0 else None,
                    hovertemplate='<b>%{customdata}</b><br>Risk Score: %{z:.3f}<extra></extra>',
                    customdata=gdf['WardName'],
                    zmin=0,
                    zmax=1
                ),
                row=row, col=col
            )
            
            # If this model is flagged as "Not Ideal", add blue outlines to the non-urban wards
            if model in not_ideal_models and urban_column is not None:
                # Get non-urban wards in the top 5
                non_urban_wards = not_ideal_models[model]
                
                # Create mask for these wards
                ward_mask = gdf['WardName'].isin(non_urban_wards)
                
                # Add a separate trace with blue outlines for these wards
                if any(ward_mask):
                    fig.add_trace(
                        go.Choroplethmapbox(
                            geojson=geojson,
                            locations=gdf[ward_mask].index,
                            z=gdf[ward_mask][model],
                            colorscale=colorscale,
                            marker_line_color=map_styling['non_urban_line_color'],  # Blue outline from theme
                            marker_line_width=map_styling['non_urban_line_width'],  # Thicker border from theme
                            showscale=False,
                            hovertemplate='<b>%{customdata}</b><br>Risk Score: %{z:.3f}<br><span style="color:blue;">Non-Urban Ward</span><extra></extra>',
                            customdata=gdf[ward_mask]['WardName'],
                            zmin=0,
                            zmax=1
                        ),
                        row=row, col=col
                    )
        
        # Update mapbox settings for each subplot - ensure styling matches original
        for i in range(1, rows * cols + 1):
            if i <= len(page_models):
                fig.update_mapboxes(
                    style=map_styling['mapbox_style'],  # Use theme map style
                    center={"lat": center_lat, "lon": center_lon},
                    zoom=zoom_level,
                    row=((i-1)//cols)+1, col=((i-1)%cols)+1
                )
        
        # Update overall layout - ensuring title matches original exactly
        fig.update_layout(
            title={
                'text': "Composite Score Distribution by Model<br><span style='font-size:16px'>Page {} of {}</span>".format(page, n_pages),
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}, # Slightly smaller main title
                'y': 0.97,  # Adjusted Y position for title
            },
            height=chart_styling['height']['map'],  # Map height from theme
            margin=chart_styling['margin']['map'],  # Map margins from theme
        )
        
        # Apply theme to figure for consistent styling
        fig = apply_theme_to_figure(fig, 'composite_map')
        
        # Generate unique filename
        session_id = getattr(data_handler, 'session_id', str(uuid.uuid4()))
        filename = "composite_map_page{}.html".format(page)
        
        # Save the figure to HTML
        html_path = create_plotly_html(fig, filename)
        
        # Get all variables used across all models
        all_variables = set()
        for formula in page_formulas:
            all_variables.update(formula['variables'])
        
        # Get full variable names for context and explanations
        full_var_names = [get_full_variable_name(var) for var in all_variables]
        
        # Create model details for each model
        model_details = []
        for i, (model, formula) in enumerate(zip(page_models, page_formulas)):
            variables = formula['variables']
            full_var_names_model = [get_full_variable_name(var) for var in variables]
            
            model_detail = {
                'model_name': model,
                'variables': variables,
                'full_variable_names': full_var_names_model,
                'is_not_ideal': model in not_ideal_models,
                'non_urban_wards': not_ideal_models.get(model, []) if model in not_ideal_models else []
            }
            model_details.append(model_detail)
        
        # Create rich context for LLM
        data_summary = {
            'current_page': page,
            'total_pages': n_pages,
            'models_on_page': len(page_models),
            'all_variable_count': len(all_variables),
            'all_variables': list(all_variables),
            'all_full_variable_names': full_var_names,
            'not_ideal_count': sum(1 for model in page_models if model in not_ideal_models)
        }
        
        visual_elements = {
            'map_type': 'choropleth',
            'color_scale': 'YlOrRd',
            'color_meaning': 'yellow to dark red (low to high risk)',
            'scale_range': '0-1 normalized risk scores',
            'layout': "{}x{} grid".format(rows, cols),
            'blue_outline': 'Indicates non-urban wards in top 5 (not ideal for prioritization)',
            'model_details': model_details
        }
        
        # Return success with pagination info
        return {
            'status': 'success',
            'message': 'Successfully created composite risk maps (page {} of {})'.format(page, n_pages),
            'image_path': html_path,
            'current_page': page,
            'total_pages': n_pages,
            'viz_type': 'composite_map',
            'data_summary': data_summary,
            'visual_elements': visual_elements,
            'model_details': model_details
        }
        
    except Exception as e:
        logger.error("Error creating composite maps: {}".format(str(e)), exc_info=True)
        import traceback
        logger.error(traceback.format_exc())
        return {
            'status': 'error',
            'message': 'Error creating composite maps: {}'.format(str(e))
        }


def create_vulnerability_map(data_handler):
    """
    Create vulnerability ranking map
    
    Args:
        data_handler: DataHandler instance
        
    Returns:
        dict: Status and visualization information
    """
    try:
        # Import box_plot_function locally to avoid circular imports
        from .charts import box_plot_function
        
        # Check if vulnerability rankings are available
        if not hasattr(data_handler, 'vulnerability_rankings') or data_handler.vulnerability_rankings is None:
            # Check if box plot function has been run
            if hasattr(data_handler, 'boxwhisker_plot') and data_handler.boxwhisker_plot:
                # Extract ward rankings from box plot data
                data_handler.vulnerability_rankings = data_handler.boxwhisker_plot['ward_rankings']
            else:
                # Try to load from file
                rankings_file = os.path.join(data_handler.session_folder, 'vulnerability_rankings.csv')
                if os.path.exists(rankings_file):
                    data_handler.vulnerability_rankings = pd.read_csv(rankings_file)
                else:
                    # If not available, try to run the box plot function to generate rankings
                    if data_handler.composite_scores is not None and 'scores' in data_handler.composite_scores:
                        box_plot_result = box_plot_function(data_handler.composite_scores['scores'])
                        if isinstance(box_plot_result, dict) and 'ward_rankings' in box_plot_result:
                            data_handler.vulnerability_rankings = box_plot_result['ward_rankings']
                            data_handler.boxwhisker_plot = box_plot_result
                        else:
                            return {
                                'status': 'error',
                                'message': 'Could not generate vulnerability rankings'
                            }
                    else:
                        return {
                            'status': 'error',
                            'message': 'Vulnerability rankings not available. Run vulnerability analysis first.'
                        }
        
        if data_handler.shapefile_data is None:
            return {
                'status': 'error',
                'message': 'Shapefile data not loaded'
            }
        
        # Get a copy of the shapefile with standardized CRS
        shapefile_data = ensure_wgs84_crs(data_handler.shapefile_data)
        
        # Ensure vulnerability_rankings has the right data types
        # Convert columns that should be numeric
        for col in ['overall_rank', 'value']:
            if col in data_handler.vulnerability_rankings.columns:
                data_handler.vulnerability_rankings[col] = pd.to_numeric(data_handler.vulnerability_rankings[col], errors='coerce')
        
        # Merge shapefile with vulnerability rankings
        gdf = shapefile_data.merge(
            data_handler.vulnerability_rankings,
            on='WardName',
            how='left'
        )
        
        # Handle any NaN values in overall_rank (wards not in the rankings)
        if 'overall_rank' in gdf.columns:
            gdf['overall_rank'] = gdf['overall_rank'].fillna(-1).astype(int)
        
        # Convert geometry to geojson with proper serialization
        gdf_prepared = prepare_geodataframe_for_json(gdf)
        geojson = json.loads(gdf_prepared.to_json())
        
        # Get proper map centering
        center_lat = gdf.geometry.centroid.y.mean()
        center_lon = gdf.geometry.centroid.x.mean()
        
        # Calculate appropriate zoom level based on the bounding box
        bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
        span_x = max(0.01, bounds[2] - bounds[0])  # Ensure minimum span to avoid zoom errors
        span_y = max(0.01, bounds[3] - bounds[1])
        
        # Calculate zoom level - ensure it's reasonable
        zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
        
        # Create choropleth map with Plotly
        fig = go.Figure()
        
        # Create hover text with proper formatting
        hover_text = []
        for i, row in gdf.iterrows():
            ward_name = row['WardName']
            rank = row['overall_rank'] if 'overall_rank' in gdf.columns and row['overall_rank'] != -1 else "Not ranked"
            category = row['vulnerability_category'] if 'vulnerability_category' in gdf.columns else "Unknown"
            hover_text.append("{}<br>Rank: {}<br>Category: {}".format(ward_name, rank, category))
        
        # Get categorical colors for vulnerability categories
        color_values = []
        if 'vulnerability_category' in gdf.columns:
            # Map categories to numeric values for colorscale
            category_map = {'High': 1, 'Medium': 2, 'Low': 3, None: 0}
            color_values = [category_map.get(cat, 0) for cat in gdf['vulnerability_category']]
            # Use a colorscale that visually distinguishes categories
            colorscale = [
                [0, 'rgba(200,200,200,0.5)'],  # Not ranked
                [0.25, '#d7191c'],  # High vulnerability (red)
                [0.5, '#fdae61'],   # Medium vulnerability (orange)
                [0.75, '#ffffbf']   # Low vulnerability (yellow)
            ]
            z_values = color_values
            tick_vals = [0.5, 1.5, 2.5]
            tick_text = ['High', 'Medium', 'Low']
        else:
            # Fallback to using overall_rank
            z_values = gdf['overall_rank'] if 'overall_rank' in gdf.columns else None
            colorscale = 'Plasma_r'  # Reverse plasma so high vulnerability (low rank) is dark
            # Determine tick values based on actual data
            max_rank = gdf['overall_rank'].max() if 'overall_rank' in gdf.columns else 100
            tick_vals = [1, max_rank / 2, max_rank]
            tick_text = ['High', 'Medium', 'Low']
        
        # Add the choropleth layer with error handling
        try:
            fig.add_trace(go.Choroplethmapbox(
                geojson=geojson,
                locations=gdf.index,
                z=z_values,
                colorscale=colorscale,
                marker_opacity=0.8,
                marker_line_width=0.5,
                marker_line_color='black',
                hovertemplate='%{hovertext}<extra></extra>',
                hovertext=hover_text,
                colorbar=dict(
                    title=dict(
                        text="Vulnerability",
                        font=dict(size=12)
                    ),
                    tickmode='array',
                    tickvals=tick_vals,
                    ticktext=tick_text
                )
            ))
        except Exception as e:
            logger.error("Error adding choropleth layer: {}".format(str(e)))
            return {
                'status': 'error',
                'message': 'Error creating vulnerability map: {}'.format(str(e))
            }
        
        # Update layout with error handling
        try:
            fig.update_layout(
                title={
                    'text': "Ward Vulnerability Map",
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 20}
                },
                mapbox=dict(
                    style="carto-positron",
                    center={"lat": center_lat, "lon": center_lon},
                    zoom=zoom_level
                ),
                margin=dict(l=20, r=20, t=80, b=20),
                autosize=True
            )
        except Exception as e:
            logger.error("Error updating layout: {}".format(str(e)))
            return {
                'status': 'error',
                'message': 'Error updating map layout: {}'.format(str(e))
            }
        
        # Create HTML file with error handling
        try:
            html_path = create_plotly_html(fig, "vulnerability_map.html")
        except Exception as e:
            logger.error("Error creating HTML file: {}".format(str(e)))
            return {
                'status': 'error',
                'message': 'Error saving vulnerability map: {}'.format(str(e))
            }
        
        # Get category counts for data summary
        category_counts = {}
        if 'vulnerability_category' in gdf.columns:
            category_counts = gdf['vulnerability_category'].value_counts().to_dict()
        
        # Prepare statistics for data summary
        statistics = {}
        if 'value' in gdf.columns:
            values = gdf['value'].dropna().values
            if len(values) > 0:
                statistics = {
                    'min_score': float(np.min(values)),
                    'max_score': float(np.max(values)),
                    'mean_score': float(np.mean(values)),
                    'median_score': float(np.median(values))
                }
        
        # Create rich context for LLM
        data_summary = {
            'ward_count': len(gdf),
            'ranked_ward_count': gdf['overall_rank'].notna().sum() if 'overall_rank' in gdf.columns else 0,
            'category_counts': category_counts,
            'statistics': statistics,
            'top_vulnerable_wards': gdf.sort_values('overall_rank')['WardName'].head(5).tolist() 
                                  if 'overall_rank' in gdf.columns else []
        }
        
        visual_elements = {
            'map_type': 'choropleth',
            'color_scale': 'Category-based coloring' if 'vulnerability_category' in gdf.columns else 'Plasma_r',
            'color_meaning': 'Darker colors = higher vulnerability',
            'scale_divisions': 'High, Medium, Low vulnerability'
        }
        
        # Return success with paths and metadata
        return {
            'status': 'success',
            'message': 'Successfully created vulnerability map',
            'image_path': html_path,
            'viz_type': 'vulnerability_map',
            'data_summary': data_summary,
            'visual_elements': visual_elements
        }
        
    except Exception as e:
        logger.error("Error creating vulnerability map: {}".format(str(e)), exc_info=True)
        import traceback
        logger.error(traceback.format_exc())
        return {
            'status': 'error',
            'message': 'Error creating vulnerability map: {}'.format(str(e))
        }


def create_urban_extent_map(data_handler, threshold=30):
    """
    Create urban extent and vulnerability map at a specific threshold.
    
    Args:
        data_handler: DataHandler instance
        threshold: Urban threshold percentage (0-100).
    
    Returns:
        dict: Status and visualization information
    """
    try:
        # Import box_plot_function locally to avoid circular imports
        from .charts import box_plot_function
        
        # Standardize and validate threshold
        current_threshold_value = float(threshold) if threshold is not None else 30.0
        current_threshold_value = max(0.0, min(100.0, current_threshold_value))  # Clamp
        
        logger.info("Creating urban extent map with threshold: {}%".format(current_threshold_value))
        
        # Essential Data Checks
        if data_handler.csv_data is None:
            return {'status': 'error', 'message': 'CSV data not loaded for urban extent map.'}
        if data_handler.shapefile_data is None:
            return {'status': 'error', 'message': 'Shapefile data not loaded for urban extent map.'}
        
        # Check for vulnerability rankings and generate if needed
        vuln_rankings = None
        if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
            vuln_rankings = data_handler.vulnerability_rankings
        elif (hasattr(data_handler, 'composite_scores') and 
              data_handler.composite_scores is not None and 
              isinstance(data_handler.composite_scores, dict) and
              'scores' in data_handler.composite_scores):
            
            # Generate vulnerability rankings
            box_plot_result = box_plot_function(data_handler.composite_scores['scores'])
            if box_plot_result.get('status') == 'success' and 'ward_rankings' in box_plot_result:
                vuln_rankings = box_plot_result['ward_rankings']
                data_handler.vulnerability_rankings = vuln_rankings
                data_handler.boxwhisker_plot = box_plot_result
        
        # Find Urban Percentage Column using vectorized operations
        urban_percent_cols = ['UrbanPercentage', 'UrbanPercent', 'Urban_Percent', 
                            'urban_percent', 'urbanPercent', 'urbanpercent', 
                            'urban_percentage', 'percent_urban']
        
        # Standardize column names for matching
        csv_columns_lower = {col.lower(): col for col in data_handler.csv_data.columns}
        
        # Try to find an urban percentage column
        urban_percent_col = None
        for potential_col in urban_percent_cols:
            if potential_col.lower() in csv_columns_lower:
                urban_percent_col = csv_columns_lower[potential_col.lower()]
                logger.info("Found urban percentage column in CSV: '{}'".format(urban_percent_col))
                break
        
        # If no percentage column found, try for binary Urban column
        if urban_percent_col is None and 'urban' in csv_columns_lower:
            urban_col_name = csv_columns_lower['urban']
            logger.info("Found binary Urban column '{}' in CSV, converting to percentage.".format(urban_col_name))
            urban_percent_col = 'UrbanPercent_Generated'
            # Create the column as a vectorized operation
            data_handler.csv_data[urban_percent_col] = data_handler.csv_data[urban_col_name].apply(
                lambda x: 100.0 if str(x).lower() in ['yes', 'true', '1', 'y'] else 0.0
            )
        
        # Fallback to shapefile
        if urban_percent_col is None and data_handler.shapefile_data is not None:
            shp_columns_lower = {col.lower(): col for col in data_handler.shapefile_data.columns}
            
            # Check in shapefile columns
            for potential_col in urban_percent_cols:
                if potential_col.lower() in shp_columns_lower:
                    shp_col_name = shp_columns_lower[potential_col.lower()]
                    logger.info("Found urban percentage column '{}' in shapefile.".format(shp_col_name))
                    urban_percent_col = 'UrbanPercent_From_Shapefile'
                    
                    # Merge to CSV data as a single vectorized operation
                    if urban_percent_col not in data_handler.csv_data.columns:
                        temp_shp_df = data_handler.shapefile_data[['WardName', shp_col_name]].rename(
                            columns={shp_col_name: urban_percent_col}
                        )
                        data_handler.csv_data = data_handler.csv_data.merge(
                            temp_shp_df, on='WardName', how='left'
                        )
                    break
            
            # Check for binary Urban column in shapefile
            if urban_percent_col is None and 'urban' in shp_columns_lower:
                urban_col_name = shp_columns_lower['urban']
                logger.info("Found binary Urban column '{}' in shapefile, converting to percentage.".format(urban_col_name))
                urban_percent_col = 'UrbanPercent_From_Shapefile'
                
                # Process as a single vectorized operation
                if urban_percent_col not in data_handler.csv_data.columns:
                    temp_shp_df = pd.DataFrame({
                        'WardName': data_handler.shapefile_data['WardName'],
                        urban_percent_col: data_handler.shapefile_data[urban_col_name].apply(
                            lambda x: 100.0 if str(x).lower() in ['yes', 'true', '1', 'y'] else 0.0
                        )
                    })
                    data_handler.csv_data = data_handler.csv_data.merge(
                        temp_shp_df, on='WardName', how='left'
                    )
        
        if urban_percent_col is None:
            return {'status': 'error', 'message': 'Urban percentage data not found. Cannot create urban extent map.'}
        
        # Ensure urban percentage column has proper numeric values
        data_handler.csv_data[urban_percent_col] = pd.to_numeric(
            data_handler.csv_data[urban_percent_col], errors='coerce'
        ).fillna(0.0)  # Fill NaNs with 0%
        
        # Prepare Merged GeoDataFrame - vectorized operations
        shapefile_data_for_merge = ensure_wgs84_crs(data_handler.shapefile_data.copy())
        urban_data_for_merge = data_handler.csv_data[['WardName', urban_percent_col]].copy()
        
        # Create merged_data with urban data
        merged_data = shapefile_data_for_merge.merge(
            urban_data_for_merge, on='WardName', how='left'
        )
        
        # Add vulnerability data if available
        if vuln_rankings is not None:
            vuln_data_for_merge = vuln_rankings[['WardName', 'overall_rank', 'vulnerability_category']].copy()
            merged_data = merged_data.merge(
                vuln_data_for_merge, on='WardName', how='left'
            )
        
        # Calculate threshold status based on urban percentage - vectorized
        threshold_str = str(current_threshold_value).replace('.', '_')
        meets_threshold_field = 'MeetsThreshold_{}'.format(threshold_str)
        merged_data[meets_threshold_field] = merged_data[urban_percent_col] >= current_threshold_value
        
        # Count wards above/below threshold
        meets_count = int(merged_data[meets_threshold_field].sum())
        below_count = int((~merged_data[meets_threshold_field]).sum())
        
        # Prepare GeoJSON and Map Centering
        gdf_prepared = prepare_geodataframe_for_json(merged_data.copy())
        geojson = json.loads(gdf_prepared.to_json())
        
        # Calculate map center and zoom level
        center_lat = float(merged_data.geometry.centroid.y.mean())
        center_lon = float(merged_data.geometry.centroid.x.mean())
        if pd.isna(center_lat) or pd.isna(center_lon): 
            center_lat, center_lon = 0.0, 0.0
        
        bounds = merged_data.geometry.total_bounds
        span_x = max(0.01, float(bounds[2]) - float(bounds[0]))
        span_y = max(0.01, float(bounds[3]) - float(bounds[1]))
        zoom_level = float(min(10, max(4, 6 - np.log(max(span_x, span_y)))))
        
        # Create Plotly figure
        fig = go.Figure()
        
        # Prepare Hover Text (for all wards) - vectorized
        hover_texts = merged_data.apply(
            lambda row: (
                "<b>{}</b><br>"
                "Urban: {:.1f}%<br>"
                "Vulnerability Rank: {}<br>"
                "Category: {}<br>"
                "Status: {}".format(
                    row['WardName'],
                    row[urban_percent_col],
                    int(row['overall_rank']) if 'overall_rank' in row and pd.notna(row['overall_rank']) else 'Not ranked',
                    row['vulnerability_category'] if 'vulnerability_category' in row and pd.notna(row['vulnerability_category']) else 'Unknown',
                    'Urban (Above Threshold)' if row[meets_threshold_field] else 'Non-Urban (Below Threshold)'
                )
            ),
            axis=1
        ).tolist()
        
        # Draw wards above threshold (colored by vulnerability if data exists)
        wards_above_threshold = merged_data[merged_data[meets_threshold_field]].copy()
        if not wards_above_threshold.empty:
            # Determine colorscale and color values based on available data
            if 'overall_rank' in wards_above_threshold.columns and vuln_rankings is not None:
                # Use vulnerability ranks for coloring
                overall_ranks_above = wards_above_threshold['overall_rank'].fillna(0).astype(float)
                color_values = overall_ranks_above.tolist()
                colorscale = 'Plasma_r'  # Reverse plasma (dark colors = high vulnerability)
                
                # Create color bar ticks
                min_r, max_r = overall_ranks_above.min(), overall_ranks_above.max()
                # Default values 
                tickvals, ticktext = [0], ["N/A"]
                
                # Better ticks if we have real data
                if min_r != max_r and min_r > 0 and max_r > 0:
                    num_ticks = min(3, int(max_r - min_r) + 1)
                    if num_ticks <= 1: 
                        num_ticks = 2
                    tickvals = np.linspace(min_r, max_r, num=num_ticks).tolist()
                    ticktext = ["Rank {}".format(int(round(t))) for t in tickvals]
                    if len(tickvals) >= 1: 
                        ticktext[0] = "High Vuln. (Rank {})".format(int(round(tickvals[0])))
                    if len(tickvals) > 1: 
                        ticktext[-1] = "Low Vuln. (Rank {})".format(int(round(tickvals[-1])))
            else:
                # Use urban percentage for coloring if no vulnerability data
                color_values = wards_above_threshold[urban_percent_col].tolist()
                colorscale = 'YlOrRd'  # Yellow-Orange-Red
                
                # Create color bar ticks for urban percentage
                min_val = min(color_values) if color_values else 0
                max_val = max(color_values) if color_values else 100
                tickvals = np.linspace(min_val, max_val, 3).tolist()
                ticktext = ["{:.0f}%".format(val) for val in tickvals]
            
            # Add the choropleth trace for above-threshold wards
            fig.add_trace(go.Choroplethmapbox(
                geojson=geojson, 
                locations=wards_above_threshold.index.tolist(),
                z=color_values, 
                colorscale=colorscale,
                marker_opacity=0.8, 
                marker_line_width=0.5, 
                marker_line_color='black',
                showscale=True,
                colorbar=dict(
                    title=dict(
                        text="Vulnerability Rank<br>(Urban Areas)" if vuln_rankings is not None 
                             else "Urban Percentage",
                        font=dict(size=10)
                    ),
                    tickmode='array', 
                    tickvals=tickvals, 
                    ticktext=ticktext, 
                    len=0.7, 
                    y=0.85
                ),
                hovertext=[hover_texts[i] for i in wards_above_threshold.index],
                hovertemplate='%{hovertext}<extra></extra>',
                name='Urban (>{}%)'.format(current_threshold_value)
            ))
        else:
            # Add annotation if no wards meet threshold
            fig.add_annotation(
                x=0.5, 
                y=0.5, 
                text="No wards meet the {}% urbanicity threshold.".format(current_threshold_value),
                showarrow=False, 
                xref="paper", 
                yref="paper", 
                font=dict(size=16, color="grey"), 
                align="center"
            )
        
        # Draw wards below threshold (grayed out)
        wards_below_threshold = merged_data[~merged_data[meets_threshold_field]].copy()
        if not wards_below_threshold.empty:
            # Add a trace with a fixed gray color for all below-threshold wards
            fig.add_trace(go.Choroplethmapbox(
                geojson=geojson, 
                locations=wards_below_threshold.index.tolist(),
                z=np.zeros(len(wards_below_threshold)),  # Dummy z values for uniform color - vectorized
                colorscale=[[0, 'rgba(200,200,200,0.4)'], [1, 'rgba(200,200,200,0.4)']],  # Light gray
                marker_opacity=0.4, 
                marker_line_width=0.2, 
                marker_line_color='rgba(150,150,150,0.3)',
                showscale=False,
                hovertext=[hover_texts[i] for i in wards_below_threshold.index],
                hovertemplate='%{hovertext}<extra></extra>',
                name='Non-Urban (<{}%)'.format(current_threshold_value)
            ))
        
        # Create title based on data
        title_main = "Urban Areas & Vulnerability (Threshold: {}%)".format(current_threshold_value)
        title_sub = "<span style='font-size:12px; color:gray;'>"
        if not wards_above_threshold.empty:
            if vuln_rankings is not None:
                title_sub += "Urban areas colored by vulnerability rank. Non-urban areas are grayed out."
            else:
                title_sub += "Urban areas colored by urban percentage. Non-urban areas are grayed out."
        elif meets_count == 0:
            title_sub += "No areas meet the urban threshold. All areas shown in gray."
        title_sub += "</span>"
        
        # Update layout
        fig.update_layout(
            title={
                'text': "{}<br>{}".format(title_main, title_sub), 
                'x': 0.5, 
                'xanchor': 'center', 
                'font': {'size': 18}
            },
            mapbox=dict(
                style="carto-positron", 
                center={"lat": center_lat, "lon": center_lon}, 
                zoom=zoom_level
            ),
            height=650, 
            margin=dict(l=20, r=20, t=100, b=50), 
            autosize=True,
            legend=dict(
                yanchor="top", 
                y=0.99, 
                xanchor="left", 
                x=0.01, 
                bgcolor='rgba(255,255,255,0.7)'
            )
        )
        
        # Generate a unique filename with random number to avoid caching issues
        threshold_str_for_filename = str(current_threshold_value).replace('.', '_')
        filename = "urban_extent_vuln_{}_{}.html".format(threshold_str_for_filename, np.random.randint(10000))
        
        # Save the HTML file
        html_path = create_plotly_html(fig, filename)
        
        # Create rich context for LLM explanation
        data_summary = {
            'threshold': float(current_threshold_value),
            'meets_threshold': meets_count,
            'below_threshold': below_count,
            'urban_percentage': float(meets_count / (meets_count + below_count) * 100) if (meets_count + below_count) > 0 else 0,
            'has_vulnerability_data': vuln_rankings is not None,
            'non_urban_high_vulnerability_wards': []
        }
        
        # Check for non-urban high vulnerability wards
        if vuln_rankings is not None:
            # Identify high vulnerability wards (top 10)
            high_vuln_wards = merged_data.sort_values('overall_rank').head(10)
            
            # Find those that are non-urban
            non_urban_high_vuln = high_vuln_wards[~high_vuln_wards[meets_threshold_field]]
            
            if not non_urban_high_vuln.empty:
                data_summary['non_urban_high_vulnerability_wards'] = non_urban_high_vuln['WardName'].tolist()
                data_summary['has_non_urban_high_vulnerability'] = True
                
                # Add rank and urban percentage for each
                non_urban_details = []
                for _, row in non_urban_high_vuln.iterrows():
                    non_urban_details.append({
                        'ward_name': row['WardName'],
                        'rank': int(row['overall_rank']) if 'overall_rank' in row and pd.notna(row['overall_rank']) else None,
                        'urban_percentage': float(row[urban_percent_col]) if urban_percent_col in row else None
                    })
                data_summary['non_urban_high_vulnerability_details'] = non_urban_details
            else:
                data_summary['has_non_urban_high_vulnerability'] = False
        else:
            data_summary['has_non_urban_high_vulnerability'] = False
        
        visual_elements = {
            'map_type': 'choropleth',
            'urban_color_scale': 'Plasma_r' if vuln_rankings is not None else 'YlOrRd',
            'color_meaning': 'Urban areas colored by vulnerability rank (darker = higher vulnerability)' 
                            if vuln_rankings is not None else 'Urban areas colored by urban percentage',
            'non_urban_appearance': 'Grayed out',
            'has_legend': True,
            'has_colorbar': not wards_above_threshold.empty
        }
        
        # Return success with rich context
        return {
            'status': 'success',
            'message': 'Urban extent & vulnerability map for {}% threshold generated.'.format(current_threshold_value),
            'image_path': html_path,
            'threshold': float(current_threshold_value),
            'meets_threshold': meets_count,
            'below_threshold': below_count,
            'viz_type': 'urban_extent_map',
            'data_summary': data_summary,
            'visual_elements': visual_elements
        }
            
    except Exception as e:
        logger.error("Error generating urban extent map: {}".format(str(e)), exc_info=True)
        import traceback
        logger.error(traceback.format_exc())
        return {
            'status': 'error',
            'message': 'Error in urban extent map generation: {}'.format(str(e))
        }


def get_available_map_types():
    """
    Get list of available map visualization types
    
    Returns:
        list: Available map types with descriptions
    """
    return [
        {
            'type': 'variable_map',
            'name': 'Variable Distribution Map',
            'description': 'Shows the distribution of a single variable across geographic areas',
            'requires_analysis': False
        },
        {
            'type': 'normalized_map',
            'name': 'Normalized Variable Map', 
            'description': 'Shows normalized variable values (0-1 scale)',
            'requires_analysis': True
        },
        {
            'type': 'composite_map',
            'name': 'Vulnerability Composite Map',
            'description': 'Shows composite vulnerability scores from analysis',
            'requires_analysis': True
        },
        {
            'type': 'vulnerability_map',
            'name': 'Vulnerability Classification Map',
            'description': 'Shows vulnerability categories (High/Medium/Low)',
            'requires_analysis': True
        },
        {
            'type': 'urban_extent_map',
            'name': 'Urban Extent Map',
            'description': 'Shows urban vs rural classification based on threshold',
            'requires_analysis': False
        }
    ]


def validate_map_inputs(data_handler, map_type, **kwargs):
    """
    Validate inputs for map creation
    
    Args:
        data_handler: DataHandler instance
        map_type: Type of map to create
        **kwargs: Additional parameters
        
    Returns:
        dict: Validation result
    """
    if data_handler is None:
        return {
            'status': 'error',
            'message': 'Data handler is required'
        }
    
    if data_handler.shapefile_data is None:
        return {
            'status': 'error',
            'message': 'Shapefile data is required for map visualizations'
        }
    
    # Type-specific validation
    if map_type == 'variable_map':
        if data_handler.csv_data is None:
            return {
                'status': 'error',
                'message': 'CSV data is required for variable maps'
            }
    
    elif map_type in ['composite_map', 'vulnerability_map']:
        # These require analysis to be complete
        if not hasattr(data_handler, 'composite_scores') or data_handler.composite_scores is None:
            return {
                'status': 'error',
                'message': 'Analysis must be completed before creating composite/vulnerability maps'
            }
    
    return {
        'status': 'success',
        'message': 'Validation passed'
    }


def get_map_summary(data_handler):
    """
    Get a summary of available map visualizations
    
    Args:
        data_handler: DataHandler instance
        
    Returns:
        dict: Summary of available maps and data status
    """
    available_maps = []
    
    # Check what maps can be created
    map_types = get_available_map_types()
    
    for map_info in map_types:
        validation = validate_map_inputs(data_handler, map_info['type'])
        map_info['available'] = validation['status'] == 'success'
        map_info['requirement_message'] = validation.get('message', '')
        available_maps.append(map_info)
    
    # Get variable information
    available_variables = []
    if data_handler.csv_data is not None:
        available_variables = [col for col in data_handler.csv_data.columns 
                             if col != 'WardName' and not is_id_column(col)]
    
    return {
        'status': 'success',
        'available_maps': available_maps,
        'available_variables': available_variables,
        'has_shapefile': data_handler.shapefile_data is not None,
        'has_csv_data': data_handler.csv_data is not None,
        'analysis_complete': hasattr(data_handler, 'composite_scores') and data_handler.composite_scores is not None
    } 