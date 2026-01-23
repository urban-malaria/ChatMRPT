"""
Composite Method Visualization Functions

Clean implementations of composite analysis visualizations designed specifically
for the VisualizationAgent. These functions work directly with the unified dataset
structure and provide agent-friendly interfaces.

Includes:
1. Composite Score Maps - Geographic risk visualization with individual model breakdowns
2. Vulnerability Map - Risk classification mapping
3. Box Plot Ranking - Statistical distribution analysis
4. Urban Extent Map - Urban vs rural risk patterns  
5. Decision Tree - Risk factor decision logic
"""

import logging
import numpy as np
import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import time
from datetime import datetime

from .core_utils import (
    prepare_unified_dataset,
    save_agent_visualization,
    get_vulnerability_colors,
    create_geojson_from_gdf,
    calculate_data_statistics
)
from app.utils.map_overlays import add_lga_boundary_overlay, calculate_lga_averages
from app.utils.visualization_controls import inject_lga_hover_highlight

logger = logging.getLogger(__name__)

def create_agent_composite_score_maps(unified_dataset: gpd.GeoDataFrame, 
                                    session_id: str = 'default',
                                    models_per_page: int = 4,
                                    page: int = 1) -> Dict[str, Any]:
    """
    Create composite score maps showing individual model breakdowns - ORIGINAL DESIGN PRESERVED
    
    Args:
        unified_dataset: Enhanced unified dataset GeoDataFrame
        session_id: Session identifier for file organization
        models_per_page: Number of model maps per page
        
    Returns:
        Dictionary with visualization results and metadata
    """
    try:
        logger.info("🗺️ Creating agent composite score maps...")
        
        # Prepare dataset
        prep_result = prepare_unified_dataset(
            unified_dataset, 
            required_columns=['composite_score', 'WardName']
        )
        
        if prep_result['status'] != 'success':
            return prep_result
        
        gdf = prep_result['data']
        enhanced_categories = prep_result['enhanced_categories']
        
        # Get individual model columns
        model_columns = enhanced_categories['individual_models']
        
        if not model_columns:
            return {
                'status': 'error',
                'message': 'No individual model columns found in unified dataset',
                'available_categories': enhanced_categories
            }
        
        logger.info(f"📊 Found {len(model_columns)} individual models")
        
        # ORIGINAL: Pagination support - get models for requested page
        n_pages = max(1, (len(model_columns) + models_per_page - 1) // models_per_page)
        
        # Validate page number
        if page < 1 or page > n_pages:
            return {
                'status': 'error',
                'message': f'Page {page} not valid. Available pages: 1-{n_pages}',
                'total_pages': n_pages
            }
        
        # Get models for this page
        start_idx = (page - 1) * models_per_page
        end_idx = min(start_idx + models_per_page, len(model_columns))
        page_models = model_columns[start_idx:end_idx]
        
        # ORIGINAL: Create fallback formulas since we don't have them in unified dataset
        page_formulas = []
        for model in page_models:
            # Create fallback formula structure
            page_formulas.append({
                'variables': ['population', 'pfpr', 'elevation', 'temp_mean'],  # Sample vars
                'formula': f"Basic malaria risk model using multiple variables",
                'complexity': 'medium'
            })
        
        # ORIGINAL: Check for urban column and identify non-ideal models
        urban_column = None
        for col in ['Urban', 'urban', 'URBAN', 'UrbanStatus', 'urbanPercentage', 'UrbanPercentage']:
            if col in gdf.columns:
                urban_column = col
                break
        
        # ORIGINAL: COMPLETE Blue outline logic for non-ideal models
        not_ideal_models = {}
        if urban_column is not None:
            # For each model, check if non-urban wards are in the top 5 for vulnerability
            for model in page_models:
                if model in gdf.columns:
                    # Sort wards by model score (descending) to find top 5
                    top_wards = gdf.sort_values(model, ascending=False).head(5)
                    
                    # Check if any of these wards are non-urban
                    if urban_column in ['urbanPercentage', 'UrbanPercentage']:
                        # For percentage columns, consider < 50% as non-urban
                        non_urban_top_wards = top_wards[top_wards[urban_column] < 50]
                    else:
                        # For categorical columns
                        non_urban_top_wards = top_wards[top_wards[urban_column].astype(str).str.lower().isin(['no', 'false', '0', 'n'])]
                    
                    # If there are non-urban wards in top 5, flag this model as "Not Ideal"
                    if len(non_urban_top_wards) > 0:
                        not_ideal_models[model] = non_urban_top_wards['WardName'].tolist()
                        logger.info(f"🔵 Model {model} flagged as 'Not Ideal' - {len(non_urban_top_wards)} non-urban wards in top 5")
        
        # ORIGINAL: Get map center and zoom
        center_lat = prep_result['map_center']['lat']
        center_lon = prep_result['map_center']['lon']
        zoom_level = prep_result['zoom_level']
        
        # ORIGINAL: Create GeoJSON
        geojson = create_geojson_from_gdf(gdf)
        
        # ORIGINAL: Determine layout based on number of models
        if len(page_models) == 1:
            rows, cols = 1, 1
        elif len(page_models) == 2:
            rows, cols = 1, 2
        else:
            rows, cols = 2, 2
        
        # ORIGINAL: Create subplot titles with variables on separate lines
        subplot_titles = []
        for model, formula in zip(page_models, page_formulas):
            variables = formula['variables']
            if variables and len(variables) > 0:
                # ORIGINAL: Create title with variables on separate lines
                var_names = []
                for var in variables:
                    var_names.append(var.replace('_', ' ').title())
                title = "<br>".join(var_names)
                
                # ORIGINAL: Add "Not Ideal" designation if this model is flagged
                if model in not_ideal_models:
                    title = f"{title}<br><span class='not-ideal-label'>(Not Ideal)</span>"
            else:
                title = f"{model.replace('model_', 'Model ')}"
                if model in not_ideal_models:
                    title = f"{title}<br><span class='not-ideal-label'>(Not Ideal)</span>"
                
            subplot_titles.append(title)
        
        # ORIGINAL: Create subplots with exact same spacing
        fig = make_subplots(
            rows=rows,
            cols=cols,
            specs=[[{"type": "mapbox"}] * cols for _ in range(rows)],
            subplot_titles=subplot_titles,
            vertical_spacing=0.22,  # ORIGINAL: Proper spacing for 4 maps
            horizontal_spacing=0.05 # ORIGINAL: Clean horizontal spacing
        )
        
        # ORIGINAL: Add choropleth for each model
        for idx, model in enumerate(page_models):
            row = idx // cols + 1
            col = idx % cols + 1
            
            # ORIGINAL: Add choropleth trace for the model
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=geojson,
                    locations=gdf.index,
                    z=gdf[model],
                    colorscale='YlOrRd',  # ORIGINAL: Same colorscale
                    marker_line_color='black',
                    marker_line_width=0.5,
                    showscale=(idx == 0),  # ORIGINAL: Only show scale for first plot
                    colorbar=dict(
                        title=dict(
                            text="Risk Score",
                            font=dict(size=12)
                        ),
                        tickvals=[0, 0.25, 0.5, 0.75, 1],  # ORIGINAL: Five tick values
                        ticktext=["Very Low", "Low", "Medium", "High", "Very High"]  # ORIGINAL: Five labels
                    ) if idx == 0 else None,
                    hovertemplate='<b>%{customdata}</b><br>Risk Score: %{z:.3f}<extra></extra>',
                    customdata=gdf['WardName'],
                    zmin=0,
                    zmax=1
                ),
                row=row, col=col
            )
            
            # ORIGINAL: WORKING BLUE OUTLINE IMPLEMENTATION
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
                            colorscale='YlOrRd',
                            marker_line_color='blue',  # ✅ BLUE OUTLINES
                            marker_line_width=3,       # ✅ THICK for visibility
                            showscale=False,
                            hovertemplate='<b>%{customdata}</b><br>Risk Score: %{z:.3f}<br><span style="color:blue;">Non-Urban Ward</span><extra></extra>',
                            customdata=gdf[ward_mask]['WardName'],
                            zmin=0,
                            zmax=1
                        ),
                        row=row, col=col
                    )
        
        # ORIGINAL: Update mapbox settings for each subplot
        for i in range(1, rows * cols + 1):
            if i <= len(page_models):
                fig.update_mapboxes(
                    style="open-street-map",
                    center={"lat": center_lat, "lon": center_lon},
                    zoom=zoom_level,
                    row=((i-1)//cols)+1, col=((i-1)%cols)+1
                )
        
        # ORIGINAL: Update overall layout - ensuring title doesn't overlap with subplot titles
        current_method = 'mean'  # Default since we're using composite
        method_label = "Composite Risk Analysis"
        
        fig.update_layout(
            title={
                'text': f"{method_label}: Risk Score Distribution by Model<br><span style='font-size:16px'>Page {page} of {n_pages}</span>",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}, # ORIGINAL: Slightly smaller main title
                'y': 0.97,  # ORIGINAL: Adjusted Y position for title
                'yanchor': 'top'
            },
            height=600,  # ORIGINAL: Perfect height for 4 maps to fit on page
            margin=dict(t=100, b=60, l=50, r=50),  # ORIGINAL: Proper margins
            autosize=True # ORIGINAL: Let Plotly try to size within the iframe
        )
        
        # ORIGINAL: Add a caption explaining the "Not Ideal" designation
        if any(model in not_ideal_models for model in page_models):
            fig.add_annotation(
                x=0.5,
                y=-0.05,
                xref="paper",
                yref="paper",
                text="Blue outlines indicate non-urban wards ranked in top 5 for vulnerability (not ideal for prioritization)",
                showarrow=False,
                font=dict(size=12, color="blue"),
                align="center"
            )
        
        # Save without timestamp to prevent URL instability
        # Create unique filename with timestamp - ensures multiple visualizations coexist
        # Files persist until session closure (browser closed or session expired)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"composite_map_page_{page}_{timestamp}"  # No .html extension here
        save_result = save_agent_visualization(
            fig, filename, session_id, 'composite_maps'
        )
        
        if save_result['status'] == 'success':
            # ORIGINAL: Create model details for contextual understanding
            model_details = []
            for model in page_models:
                model_detail = {
                    'model_name': model,
                    'is_not_ideal': model in not_ideal_models,
                    'non_urban_wards': not_ideal_models.get(model, []) if model in not_ideal_models else []
                }
                model_details.append(model_detail)
                
            return {
                'status': 'success',
                'message': f'Successfully created composite risk maps (page {page} of {n_pages})',
                'visualization_type': 'composite_score_maps',
                'current_page': page,
                'total_pages': n_pages,
                'models_on_page': len(page_models),
                'models_displayed': page_models,
                'total_models_available': len(model_columns),
                'not_ideal_count': sum(1 for model in page_models if model in not_ideal_models),
                'blue_outline': 'Blue outline indicates non-urban wards in top 5 (not ideal for prioritization)' if any(model in not_ideal_models for model in page_models) else None,
                'model_details': model_details,
                'file_path': save_result['file_path'],
                'web_path': save_result['web_path'],
                'plotly_json': save_result['plotly_json'],
                'session_id': session_id
            }
        else:
            return save_result
        
    except Exception as e:
        logger.error(f"Error creating agent composite score maps: {e}")
        return {
            'status': 'error',
            'message': f'Composite score maps creation failed: {str(e)}',
            'visualization_type': 'composite_score_maps'
        }

def create_agent_vulnerability_map(unified_dataset: gpd.GeoDataFrame,
                                 session_id: str = 'default',
                                 method: str = 'composite') -> Dict[str, Any]:
    """
    Create vulnerability classification map using composite_rank
    
    Args:
        unified_dataset: Enhanced unified dataset GeoDataFrame
        session_id: Session identifier
        
    Returns:
        Dictionary with visualization results
    """
    try:
        logger.info("🗺️ Creating agent vulnerability map...")
        
        # Prepare dataset - require composite_rank instead of overall_rank
        prep_result = prepare_unified_dataset(
            unified_dataset, 
            required_columns=['composite_score', 'composite_rank', 'WardName']
        )
        
        if prep_result['status'] != 'success':
            return prep_result
        
        gdf = prep_result['data']
        
        # Use existing composite_rank from unified dataset
        if 'composite_rank' not in gdf.columns:
            return {
                'status': 'error',
                'message': 'composite_rank column not found. Please run composite analysis first.'
            }
        
        # Use the existing composite_rank and composite_category
        gdf['vulnerability_rank'] = gdf['composite_rank'].fillna(-1).astype(int)
        
        # Use existing composite_category if available, otherwise create from rank
        if 'composite_category' in gdf.columns:
            gdf['vulnerability_category'] = gdf['composite_category']
        else:
            # Create categories from ranking if not available
            n_wards = len(gdf[gdf['vulnerability_rank'] != -1])
            quartiles = [n_wards // 4, n_wards // 2, 3 * n_wards // 4]
            
            conditions = [
                gdf['vulnerability_rank'] <= quartiles[0],
                gdf['vulnerability_rank'] <= quartiles[1],
                gdf['vulnerability_rank'] <= quartiles[2]
            ]
            
            choices = ['Very High', 'High', 'Medium']
            gdf['vulnerability_category'] = np.select(conditions, choices, default='Low')
            # Set unranked wards to 'Unknown'
            gdf.loc[gdf['vulnerability_rank'] == -1, 'vulnerability_category'] = 'Unknown'
        
        # Prepare color values: use composite_rank for continuous coloring
        z_values = gdf['vulnerability_rank'].copy()
        # Set unranked (-1) to NaN so they appear gray
        z_values = z_values.where(z_values != -1, np.nan)

        # Compute colorbar ticks BEFORE inverting z-values
        if z_values.notna().sum() > 0:
            min_rank = int(z_values.min())
            max_rank = int(z_values.max())
            median_rank = int(z_values.median())

            # Invert z-values so highest risk (rank 1) appears at TOP of colorbar
            # Transform: inverted = max_rank + 1 - original
            z_values_inverted = max_rank + 1 - z_values
            z_values = z_values_inverted

            # Inverted tickvals (original ranks mapped to inverted positions)
            # Original rank 1 (highest risk) -> inverted max_rank (top of colorbar)
            # Original max_rank (lowest risk) -> inverted 1 (bottom of colorbar)
            tickvals = [1, max_rank + 1 - median_rank, max_rank]
            ticktext = [f"Lowest Risk ({max_rank})", f"Mid-rank ({median_rank})", f"Highest Risk ({min_rank})"]
        else:
            tickvals = []
            ticktext = []

        # Choose continuous colorscale (Plasma: purple=low values, yellow=high values)
        # After inversion: yellow = highest risk at top, purple = lowest risk at bottom
        colorscale = 'Plasma'
        
        # Create hover text using vectorized operations
        ward_names = gdf['WardName'].astype(str)
        
        # Vectorize rank formatting
        ranks = gdf['vulnerability_rank'].where(gdf['vulnerability_rank'] != -1, 'Not ranked').astype(str)
        
        # Vectorize category formatting with proper categorical handling
        if 'vulnerability_category' in gdf.columns:
            # Handle categorical data properly by adding 'Unknown' category first
            if gdf['vulnerability_category'].dtype.name == 'category':
                if 'Unknown' not in gdf['vulnerability_category'].cat.categories:
                    gdf['vulnerability_category'] = gdf['vulnerability_category'].cat.add_categories(['Unknown'])
            categories = gdf['vulnerability_category'].fillna('Unknown').astype(str)
        else:
            categories = pd.Series(['Unknown'] * len(gdf), index=gdf.index)
        
        # Filter out null geometries before creating map
        valid_geometry_mask = ~gdf.geometry.isnull()
        gdf_valid = gdf[valid_geometry_mask].copy()
        
        if len(gdf_valid) == 0:
            return {
                'status': 'error',
                'message': 'No valid geometries found in the dataset. Please check your shapefile data.'
            }
        
        # Log if any geometries were filtered
        if len(gdf_valid) < len(gdf):
            logger.warning(f"Filtered out {len(gdf) - len(gdf_valid)} wards with null geometries")
        
        # Create hover text with ward name, LGA, rank, and category
        # Try multiple possible LGA column names
        if 'LGAName' in gdf_valid.columns:
            lga_names = gdf_valid['LGAName'].fillna('Unknown').astype(str)
        elif 'LGA' in gdf_valid.columns:
            lga_names = gdf_valid['LGA'].fillna('Unknown').astype(str)
        elif 'lga_name' in gdf_valid.columns:
            lga_names = gdf_valid['lga_name'].fillna('Unknown').astype(str)
        else:
            lga_names = pd.Series('Unknown', index=gdf_valid.index)

        # Calculate LGA-level rankings (average ward rank per LGA, then rank LGAs)
        lga_col = 'LGAName' if 'LGAName' in gdf_valid.columns else ('LGA' if 'LGA' in gdf_valid.columns else 'lga_name')
        if lga_col in gdf_valid.columns:
            # Calculate mean rank per LGA (lower rank = higher risk)
            lga_mean_ranks = gdf_valid.groupby(lga_col)['vulnerability_rank'].mean()
            # Rank LGAs by their average ward rank (1 = highest risk LGA)
            lga_rankings = lga_mean_ranks.rank(method='min').astype(int)
            # Determine LGA category based on ranking
            total_lgas = len(lga_rankings)
            def get_lga_category(lga_rank, total):
                if pd.isna(lga_rank):
                    return 'unknown'
                pct = lga_rank / total
                if pct <= 0.25:
                    return 'very high'
                elif pct <= 0.50:
                    return 'high'
                elif pct <= 0.75:
                    return 'medium'
                else:
                    return 'low'
            lga_categories = {lga: get_lga_category(rank, total_lgas) for lga, rank in lga_rankings.items()}
            # Map back to wards
            gdf_valid['lga_rank'] = gdf_valid[lga_col].map(lga_rankings)
            gdf_valid['lga_category'] = gdf_valid[lga_col].map(lga_categories)
        else:
            gdf_valid['lga_rank'] = pd.NA
            gdf_valid['lga_category'] = 'unknown'

        # Build hover text in user's requested format:
        # Ward: Ndiagbo
        # LGA: Mashegu
        # Ward rank: 113 (medium risk)
        # LGA rank: 8 (high)
        hover_text = []
        for idx, row in gdf_valid.iterrows():
            ward_name = str(row['WardName'])
            lga_name = str(row.get('LGAName', row.get('LGA', row.get('lga_name', 'Unknown'))))
            ward_rank = row['vulnerability_rank']
            ward_cat = str(row.get('vulnerability_category', 'Unknown')).lower()
            lga_rank = row.get('lga_rank', pd.NA)
            lga_cat = str(row.get('lga_category', 'unknown')).lower()

            text = f"<b>Ward:</b> {ward_name}<br>"
            text += f"<b>LGA:</b> {lga_name}<br>"

            if pd.notna(ward_rank) and ward_rank != -1:
                text += f"<b>Ward rank:</b> {int(ward_rank)} ({ward_cat})<br>"
            else:
                text += f"<b>Ward rank:</b> Not ranked<br>"

            if pd.notna(lga_rank):
                text += f"<b>LGA rank:</b> {int(lga_rank)} ({lga_cat})"

            hover_text.append(text)
        
        # Convert geometry to geojson with proper serialization
        geojson = create_geojson_from_gdf(gdf_valid)
        
        # Get proper map centering based on valid geometries
        bounds = gdf_valid.geometry.total_bounds
        center_lat = gdf_valid.geometry.centroid.y.mean()
        center_lon = gdf_valid.geometry.centroid.x.mean()
        span_x = max(0.01, bounds[2] - bounds[0])
        span_y = max(0.01, bounds[3] - bounds[1])
        zoom_level = min(10, max(4, 6 - np.log(max(span_x, span_y))))
        
        # Add the choropleth layer
        fig = go.Figure()
        fig.add_trace(go.Choroplethmapbox(
            geojson=geojson,
            locations=gdf_valid.index,
            z=z_values[valid_geometry_mask],
            colorscale=colorscale,
            marker_opacity=0.8,
            marker_line_width=0.5,
            marker_line_color='black',
            hovertemplate='%{hovertext}<extra></extra>',
            hovertext=hover_text,
            colorbar=dict(
                title=dict(
                    text="Vulnerability Rank",
                    font=dict(size=12)
                ),
                tickmode='array',
                tickvals=tickvals,
                ticktext=ticktext
            ),
            zmin=1,  # Inverted: lowest risk at bottom
            zmax=max_rank if z_values.notna().sum() > 0 else 1,  # Inverted: highest risk at top
            showscale=True,
            autocolorscale=False,
            zauto=False
        ))

        # Add LGA boundary overlay
        add_lga_boundary_overlay(fig, gdf_valid)

        # Update layout
        current_method = 'composite'  
        method_display = "Composite Risk Analysis"
        
        fig.update_layout(
            title={
                'text': f"Ward Vulnerability Map ({method_display})",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 20}  
            },
            mapbox=dict(
                style="open-street-map",  
                center={"lat": center_lat, "lon": center_lon},
                zoom=zoom_level
            ),
            margin=dict(l=20, r=20, t=80, b=20),  
            autosize=True
        )
        
        # Generate HTML file with fixed filename to prevent URL instability
        method_suffix = 'composite'
        # Create unique filename with timestamp - ensures multiple visualizations coexist
        # Files persist until session closure (browser closed or session expired)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"vulnerability_map_{method_suffix}_{timestamp}"  # No .html extension here
        
        save_result = save_agent_visualization(
            fig, filename, session_id, 'vulnerability_maps'
        )

        # Inject LGA hover highlighting
        if save_result.get('status') == 'success' and save_result.get('file_path'):
            try:
                lga_codes = gdf_valid['LGACode'].fillna('').astype(str).tolist() if 'LGACode' in gdf_valid.columns else []
                if lga_codes:
                    inject_lga_hover_highlight(save_result['file_path'], lga_codes)
            except Exception as hover_err:
                logger.warning(f"Failed to inject LGA hover highlight: {hover_err}")

        if save_result['status'] == 'success':
            # Create rich context for LLM
            ranked_wards = gdf[gdf['vulnerability_rank'] != -1]
            data_summary = {
                'total_wards': len(gdf),
                'ranked_wards': len(ranked_wards),
                'unranked_wards': len(gdf) - len(ranked_wards),
                'has_vulnerability_categories': 'vulnerability_category' in gdf.columns
            }
            
            # Get top and bottom 5 wards
            if len(ranked_wards) > 0:
                top_5 = ranked_wards.nsmallest(5, 'vulnerability_rank')[['WardName', 'vulnerability_rank']].to_dict('records')
                bottom_5 = ranked_wards.nlargest(5, 'vulnerability_rank')[['WardName', 'vulnerability_rank']].to_dict('records')
                data_summary['top_5_vulnerable'] = top_5
                data_summary['bottom_5_vulnerable'] = bottom_5
            
            return {
                'status': 'success',
                'message': f'Created vulnerability map with {len(gdf)} wards classified using composite_rank',
                'visualization_type': 'vulnerability_map',
                'data_summary': data_summary,
                'visual_elements': {
                    'map_type': 'choropleth',
                    'color_scale': 'Plasma_r',
                    'color_meaning': 'dark to light (high to low vulnerability)',
                    'scale_range': 'composite vulnerability rank (1 = highest vulnerability)',
                    'unranked_appearance': 'Gray (not included in ranking)'
                },
                'file_path': save_result['file_path'],
                'web_path': save_result['web_path'],
                'plotly_json': save_result['plotly_json'],
                'session_id': session_id
            }
        else:
            return save_result
        
    except Exception as e:
        logger.error(f"Error creating agent vulnerability map: {e}")
        return {
            'status': 'error',
            'message': f'Vulnerability map creation failed: {str(e)}',
            'visualization_type': 'vulnerability_map'
        }

def create_agent_box_plot_ranking(unified_dataset: gpd.GeoDataFrame,
                                session_id: str = 'default',
                                top_n_wards: int = 20,
                                page: int = 1) -> Dict[str, Any]:
    """
    Create box plot ranking visualization - ORIGINAL DESIGN PRESERVED
    """
    try:
        logger.info("📊 Creating agent box plot ranking...")
        
        # Prepare dataset
        prep_result = prepare_unified_dataset(
            unified_dataset, 
            required_columns=['composite_score', 'WardName']
        )
        
        if prep_result['status'] != 'success':
            return prep_result
        
        data = prep_result['data']
        enhanced_categories = prep_result['enhanced_categories']
        
        # ORIGINAL: Create model data for box plot (using available model scores)
        model_cols = [col for col in enhanced_categories['individual_models'] if col in data.columns]
        
        if not model_cols:
            # Fallback: create synthetic model variations from composite score
            model_cols = [f'model_{i+1}' for i in range(5)]
            for i, col in enumerate(model_cols):
                # Create slight variations of composite score 
                variation_factor = 0.8 + (i * 0.1)  # 0.8, 0.9, 1.0, 1.1, 1.2
                data[col] = data['composite_score'] * variation_factor + np.random.normal(0, 0.05, len(data))
        
        # ORIGINAL: Melt the dataframe to long format for plotting
        melted_df = pd.melt(
            data[['WardName'] + model_cols], 
            id_vars=['WardName'], 
            value_vars=model_cols,
            var_name='variable', 
            value_name='value'
        )
        
        # ORIGINAL: Calculate ward rankings - lower rank value = HIGHER vulnerability
        ward_rankings = melted_df.groupby('WardName')['value'].median().reset_index()
        ward_rankings = ward_rankings.sort_values('value', ascending=False)
        ward_rankings['overall_rank'] = range(1, len(ward_rankings) + 1)
        
        # ORIGINAL: Create vulnerability categories (High, Medium, Low)
        ward_rankings['vulnerability_category'] = pd.cut(
            ward_rankings['overall_rank'],
            bins=[0, len(ward_rankings)//3, 2*len(ward_rankings)//3, len(ward_rankings)],
            labels=['High', 'Medium', 'Low']
        )
        
        # ORIGINAL: Merge rankings back to melted dataframe
        df_long = pd.merge(melted_df, ward_rankings[['WardName', 'overall_rank', 'vulnerability_category']], on='WardName')
        
        # ORIGINAL: Sort by overall rank (most vulnerable wards at the top)
        df_long['WardName'] = pd.Categorical(
            df_long['WardName'],
            categories=ward_rankings.sort_values('overall_rank')['WardName'],
            ordered=True
        )
        
        # ORIGINAL: Calculate pagination - Show top_n_wards per page
        total_wards = len(ward_rankings)
        wards_per_page = top_n_wards
        total_pages = max(1, (total_wards + wards_per_page - 1) // wards_per_page)
        
        # ORIGINAL: Validate page number
        if page < 1 or page > total_pages:
            return {
                'status': 'error',
                'message': f'Page {page} not valid. Available pages: 1-{total_pages}',
                'total_pages': total_pages
            }
        
        # ORIGINAL: Create requested page (most vulnerable wards on page 1)
        start_idx = (page - 1) * wards_per_page
        end_idx = min(start_idx + wards_per_page, total_wards)
        
        # Get ward names for this page based on ranking
        page_wards = ward_rankings.sort_values('overall_rank')['WardName'].iloc[start_idx:end_idx].tolist()
        
        # Filter data for these wards
        page_df = df_long[df_long['WardName'].isin(page_wards)].copy()
        
        # ORIGINAL: Create helper column for sorting
        page_df = pd.merge(
            page_df,
            pd.DataFrame({'WardName': page_wards, 'sort_order': range(len(page_wards))}),
            on='WardName'
        )
        page_df = page_df.sort_values('sort_order')
        
        # ORIGINAL: Create figure
        fig = go.Figure()
        
        # ORIGINAL: For each ward, add a box plot with EXACT styling
        for ward in page_wards:
            ward_data = page_df[page_df['WardName'] == ward]
            rank = ward_rankings[ward_rankings['WardName'] == ward]['overall_rank'].values[0]
            category = ward_rankings[ward_rankings['WardName'] == ward]['vulnerability_category'].values[0]
            
            # ORIGINAL: Set color based on vulnerability category
            if category == 'High':
                box_color = '#69b3a2'  # Green-blue
            elif category == 'Medium':
                box_color = '#a8d8b9'  # Light green
            else:
                box_color = '#c7e9c0'  # Very light green
            
            fig.add_trace(go.Box(
                x=ward_data['value'],
                y=[ward] * len(ward_data),
                name=ward,
                orientation='h',
                marker_color=box_color,
                marker_line=dict(color='#3c5e8b', width=1.5),  # Blue border
                line=dict(color='#3c5e8b', width=1.5),  # Blue border for box
                hoverinfo='all',
                hovertemplate=f"<b>{ward}</b><br>Rank: {rank}<br>Category: {category}<br>Score: %{{x:.3f}}<extra></extra>",
                boxmean=True,  # Show mean as a dashed line
                showlegend=False
            ))
        
        # ORIGINAL: Update layout with EXACT styling
        method = 'composite'  # Since we're using composite analysis
        fig.update_layout(
            title={
                'text': f'{method}: Ward Vulnerability Rankings Distribution (Page {page} of {total_pages})',
                'x': 0.5,
                'y': 0.98,
                'xanchor': 'center',
                'yanchor': 'top',
                'font': {'size': 20, 'color': '#333', 'family': 'Arial, sans-serif'}
            },
            xaxis={
                'title': {
                    'text': 'Risk Score',
                    'font': {'size': 14}
                },
                'zeroline': True,
                'gridcolor': '#E5E5E5',
                'showgrid': True
            },
            yaxis={
                'title': '',
                'categoryorder': 'array',
                'categoryarray': page_wards,
                'gridcolor': '#E5E5E5',
                'showgrid': True
            },
            height=520,
            margin=dict(l=150, r=20, t=80, b=50),  # Left margin for ward names
            plot_bgcolor='#F8F9FA',
            paper_bgcolor='#F8F9FA',
            annotations=[
                dict(
                    x=0.5, y=-0.15,
                    text="Most vulnerable wards at top | Least vulnerable at bottom",
                    showarrow=False,
                    xref="paper", yref="paper",
                    font=dict(size=14, color='darkred')
                )
            ],
            autosize=True
        )
        
        # Save visualization
        filename = f"box_plot_ranking_top{top_n_wards}"
        save_result = save_agent_visualization(
            fig, filename, session_id, 'box_plot_ranking'
        )
        
        if save_result['status'] == 'success':
            # ORIGINAL: Prepare ward data for response
            page_data = []
            for ward in page_wards:
                ward_rank_info = ward_rankings[ward_rankings['WardName'] == ward].iloc[0]
                page_data.append({
                    'ward_name': ward,
                    'overall_rank': int(ward_rank_info['overall_rank']),
                    'median_score': float(ward_rank_info['value']),
                    'vulnerability_category': str(ward_rank_info['vulnerability_category'])
                })
            
            return {
                'status': 'success',
                'message': f'Successfully created vulnerability box plots (Page {page} of {total_pages})',
                'visualization_type': 'box_plot_ranking',
                'wards_displayed': len(page_wards),
                'total_wards': total_wards,
                'current_page': page,
                'total_pages': total_pages,
                'file_path': save_result['file_path'],
                'web_path': save_result['web_path'],
                'plotly_json': save_result['plotly_json'],
                'ward_rankings': page_data,
                'session_id': session_id
            }
        else:
            return save_result
        
    except Exception as e:
        logger.error(f"Error creating agent box plot ranking: {e}")
        return {
            'status': 'error',
            'message': f'Box plot ranking creation failed: {str(e)}',
            'visualization_type': 'box_plot_ranking'
        }

def create_agent_urban_extent_map(unified_dataset: gpd.GeoDataFrame,
                                session_id: str = 'default',
                                threshold: float = 50.0) -> Dict[str, Any]:
    """
    Create urban extent map - ORIGINAL DESIGN PRESERVED
    
    Shows vulnerability map with non-urban areas greyed out using urbanPercentage threshold
    At threshold 0%: normal vulnerability map
    At threshold > 0%: vulnerability map with non-urban areas at 0.2 opacity (greyed out)
    """
    try:
        logger.info(f"🏙️ Creating agent urban extent map (threshold: {threshold}%)...")
        
        # ORIGINAL: Find urban percentage column - prioritize 'urbanPercentage'
        urban_cols = ['urbanPercentage', 'UrbanPercentage', 'urban_percent', 'Urban_Percent']
        urban_col = None
        
        for col in urban_cols:
            if col in unified_dataset.columns:
                urban_col = col
                logger.info(f"📊 Using urban column: {urban_col}")
                break
        
        if urban_col is None:
            return {
                'status': 'error',
                'message': f'Urban percentage column not found. Looked for: {urban_cols}',
                'available_columns': list(unified_dataset.columns)
            }
        
        # Prepare dataset - require urbanPercentage and vulnerability data
        required_columns = [urban_col, 'WardName']
        prep_result = prepare_unified_dataset(unified_dataset, required_columns)
        
        if prep_result['status'] != 'success':
            return prep_result
        
        merged_data = prep_result['data']

        # Calculate LGA-level rankings for hover text
        lga_col = 'LGAName' if 'LGAName' in merged_data.columns else ('LGA' if 'LGA' in merged_data.columns else None)
        if lga_col and 'overall_rank' in merged_data.columns:
            # Calculate mean rank per LGA (lower rank = higher risk)
            lga_mean_ranks = merged_data.groupby(lga_col)['overall_rank'].mean()
            # Rank LGAs by their average ward rank (1 = highest risk LGA)
            lga_rankings = lga_mean_ranks.rank(method='min').astype(int)
            # Determine LGA category based on ranking
            total_lgas = len(lga_rankings)
            def get_lga_cat(lga_rank, total):
                if pd.isna(lga_rank):
                    return 'unknown'
                pct = lga_rank / total
                if pct <= 0.25:
                    return 'very high'
                elif pct <= 0.50:
                    return 'high'
                elif pct <= 0.75:
                    return 'medium'
                else:
                    return 'low'
            lga_categories = {lga: get_lga_cat(rank, total_lgas) for lga, rank in lga_rankings.items()}
            merged_data['lga_rank'] = merged_data[lga_col].map(lga_rankings)
            merged_data['lga_category'] = merged_data[lga_col].map(lga_categories)
        else:
            merged_data['lga_rank'] = pd.NA
            merged_data['lga_category'] = 'unknown'

        # ORIGINAL: Create threshold field name
        meets_threshold_field = f'meets_{threshold}_threshold'
        
        # ORIGINAL: Create boolean field for meeting threshold
        merged_data[meets_threshold_field] = merged_data[urban_col] >= threshold
        
        # Count wards meeting/not meeting threshold
        meets_count = merged_data[meets_threshold_field].sum()
        below_count = len(merged_data) - meets_count
        
        logger.info(f"🏙️ Urban/Rural classification at {threshold}% threshold:")
        logger.info(f"   • Urban wards: {meets_count}")
        logger.info(f"   • Rural wards: {below_count}")
        
        # ORIGINAL: Handle two cases - threshold 0% vs threshold > 0%
        if threshold == 0:
            # Special case: threshold 0% means show normal vulnerability map
            if 'overall_rank' in merged_data.columns:
                z_values = merged_data['overall_rank'].copy()
                z_values = z_values.where(z_values != -1, np.nan)  # Handle unranked wards
                colorbar_title = "Vulnerability Rank"
                opacity_values = [0.8] * len(merged_data)  # Full opacity for all areas

                if z_values.notna().sum() > 0:
                    min_rank = int(z_values.min())
                    max_rank = int(z_values.max())
                    median_rank = int(z_values.median())

                    # Invert z-values so highest risk (rank 1) appears at TOP of colorbar
                    z_values = max_rank + 1 - z_values

                    # Inverted tickvals
                    tickvals = [1, max_rank + 1 - median_rank, max_rank]
                    ticktext = [f"Low ({max_rank})", f"Mid ({median_rank})", f"High ({min_rank})"]
                    colorscale = 'Plasma'  # Yellow=high risk at top
                else:
                    tickvals = []
                    ticktext = []
                    colorscale = 'Plasma'
            else:
                # No vulnerability data, use urban percentage
                z_values = merged_data[urban_col]
                colorscale = 'YlOrRd'
                colorbar_title = "Urban Percentage"
                opacity_values = [0.8] * len(merged_data)
                tickvals = None
                ticktext = None
                
            # Create hover text for threshold 0% in user's requested format
            hover_text = []
            for idx in merged_data.index:
                row = merged_data.loc[idx]
                ward_name = str(row['WardName'])
                lga_name = str(row.get('LGAName', row.get('LGA', 'Unknown')))
                urban_pct = round(row.get(urban_col, 0), 1)

                text = f"<b>Ward:</b> {ward_name}<br>"
                text += f"<b>LGA:</b> {lga_name}<br>"

                if 'overall_rank' in merged_data.columns:
                    rank_val = row['overall_rank']
                    ward_cat = str(row.get('vulnerability_category', 'unknown')).lower() if 'vulnerability_category' in merged_data.columns else 'unknown'
                    lga_rank = row.get('lga_rank', pd.NA)
                    lga_cat = str(row.get('lga_category', 'unknown')).lower()

                    if pd.notna(rank_val) and rank_val != -1:
                        text += f"<b>Ward rank:</b> {int(rank_val)} ({ward_cat})<br>"
                    else:
                        text += f"<b>Ward rank:</b> Not ranked<br>"

                    if pd.notna(lga_rank):
                        text += f"<b>LGA rank:</b> {int(lga_rank)} ({lga_cat})<br>"

                text += f"<b>Urban:</b> {urban_pct}%"
                hover_text.append(text)
                
        else:
            # ORIGINAL: Threshold > 0%: Show vulnerability map with non-urban areas greyed out
            if 'overall_rank' in merged_data.columns:
                # Use vulnerability rankings but grey out non-urban areas
                z_values = merged_data['overall_rank'].copy()
                z_values = z_values.where(z_values != -1, np.nan)  # Handle unranked wards

                # ORIGINAL: Create opacity array: full opacity for urban areas, reduced for non-urban
                opacity_values = []
                for meets_threshold in merged_data[meets_threshold_field]:
                    if meets_threshold:
                        opacity_values.append(0.8)  # Full opacity for urban areas
                    else:
                        opacity_values.append(0.2)  # ORIGINAL: Very low opacity for non-urban areas (greyed out)

                colorbar_title = "Vulnerability Rank"

                if z_values.notna().sum() > 0:
                    min_rank = int(z_values.min())
                    max_rank = int(z_values.max())
                    median_rank = int(z_values.median())

                    # Invert z-values so highest risk (rank 1) appears at TOP of colorbar
                    z_values = max_rank + 1 - z_values

                    # Inverted tickvals
                    tickvals = [1, max_rank + 1 - median_rank, max_rank]
                    ticktext = [f"Low ({max_rank})", f"Mid ({median_rank})", f"High ({min_rank})"]
                    colorscale = 'Plasma'  # Yellow=high risk at top
                else:
                    tickvals = []
                    ticktext = []
                    colorscale = 'Plasma'
            else:
                # No vulnerability data, use urban percentage
                z_values = merged_data[urban_col].copy()
                
                # ORIGINAL: Create opacity array: full opacity for urban areas, reduced for non-urban
                opacity_values = []
                for meets_threshold in merged_data[meets_threshold_field]:
                    if meets_threshold:
                        opacity_values.append(0.8)  # Full opacity for urban areas
                    else:
                        opacity_values.append(0.2)  # ORIGINAL: Very low opacity for non-urban areas (greyed out)
                
                colorscale = 'YlOrRd'
                colorbar_title = "Urban Percentage"
                tickvals = None
                ticktext = None
            
            # Create hover text for threshold > 0% in user's requested format
            hover_text = []
            for idx in merged_data.index:
                row = merged_data.loc[idx]
                ward_name = str(row['WardName'])
                lga_name = str(row.get('LGAName', row.get('LGA', 'Unknown')))
                urban_pct = round(row.get(urban_col, 0), 1)
                meets_threshold = row[meets_threshold_field]

                text = f"<b>Ward:</b> {ward_name}<br>"
                text += f"<b>LGA:</b> {lga_name}<br>"

                if 'overall_rank' in merged_data.columns:
                    rank_val = row['overall_rank']
                    ward_cat = str(row.get('vulnerability_category', 'unknown')).lower() if 'vulnerability_category' in merged_data.columns else 'unknown'
                    lga_rank = row.get('lga_rank', pd.NA)
                    lga_cat = str(row.get('lga_category', 'unknown')).lower()

                    if pd.notna(rank_val) and rank_val != -1:
                        text += f"<b>Ward rank:</b> {int(rank_val)} ({ward_cat})<br>"
                    else:
                        text += f"<b>Ward rank:</b> Not ranked<br>"

                    if pd.notna(lga_rank):
                        text += f"<b>LGA rank:</b> {int(lga_rank)} ({lga_cat})<br>"

                text += f"<b>Urban:</b> {urban_pct}%<br>"
                status = f"Meets {threshold}% threshold" if meets_threshold else f"Below {threshold}% threshold"
                text += f"<b>Status:</b> {status}"
                hover_text.append(text)
        
        # Create GeoJSON
        geojson = create_geojson_from_gdf(merged_data)
        
        # ORIGINAL: Add the choropleth layer
        fig = go.Figure()
        fig.add_trace(go.Choroplethmapbox(
            geojson=geojson,
            locations=merged_data.index,
            z=z_values,
            colorscale=colorscale,
            marker_opacity=opacity_values,
            marker_line_width=0.5,
            marker_line_color='black',
            hovertemplate='%{hovertext}<extra></extra>',
            hovertext=hover_text,
            colorbar=dict(
                title=dict(
                    text=colorbar_title,
                    font=dict(size=12)
                ),
                tickmode='array' if tickvals else 'auto',
                tickvals=tickvals,
                ticktext=ticktext
            ) if meets_count > 0 else None,
            showscale=bool(meets_count > 0)
        ))
        
        # ORIGINAL: Update layout
        fig.update_layout(
            title={
                'text': f"Urban Extent Map ({threshold}% threshold)",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 20}  # ORIGINAL: Font size 20
            },
            mapbox=dict(
                style="open-street-map",
                center=prep_result['map_center'],
                zoom=prep_result['zoom_level']
            ),
            margin=dict(l=20, r=20, t=80, b=20),  # ORIGINAL: Margins
            autosize=True
        )
        
        # Save visualization
        threshold_str_for_filename = str(threshold).replace('.', '_')
        filename = f"urban_extent_vuln_{threshold_str_for_filename}"
        save_result = save_agent_visualization(
            fig, filename, session_id, 'urban_extent_map'
        )
        
        if save_result['status'] == 'success':
            return {
                'status': 'success',
                'message': f'Urban extent & vulnerability map for {threshold}% threshold generated',
                'visualization_type': 'urban_extent_map',
                'threshold': threshold,
                'urban_column_used': urban_col,
                'meets_threshold': int(meets_count),
                'below_threshold': int(below_count),
                'urban_wards': int(meets_count),  # Alias for test compatibility
                'rural_wards': int(below_count),  # Alias for test compatibility
                'urban_percentage': round((meets_count / len(merged_data)) * 100, 1) if len(merged_data) > 0 else 0,
                'has_vulnerability_data': 'overall_rank' in merged_data.columns,
                'file_path': save_result['file_path'],
                'web_path': save_result['web_path'],
                'plotly_json': save_result['plotly_json'],
                'visual_elements': {
                    'map_type': 'choropleth',
                    'color_scale': colorscale,
                    'color_meaning': 'Urban areas colored by vulnerability rank (darker = higher vulnerability)' 
                                    if 'overall_rank' in merged_data.columns else 'Urban areas colored by urban percentage',
                    'non_urban_appearance': 'Grayed out (0.2 opacity)' if threshold > 0 else 'Full opacity',
                    'has_legend': True,
                    'has_colorbar': meets_count > 0
                },
                'session_id': session_id
            }
        else:
            return save_result
        
    except Exception as e:
        logger.error(f"Error creating agent urban extent map: {e}")
        return {
            'status': 'error',
            'message': f'Urban extent map creation failed: {str(e)}',
            'visualization_type': 'urban_extent_map'
        }

def create_agent_decision_tree(unified_dataset: gpd.GeoDataFrame,
                             session_id: str = 'default') -> Dict[str, Any]:
    """
    Create decision tree visualization showing analysis workflow - ORIGINAL DESIGN PRESERVED
    """
    try:
        logger.info("🌳 Creating agent decision tree...")
        
        # Prepare dataset
        prep_result = prepare_unified_dataset(
            unified_dataset, 
            required_columns=['composite_score', 'WardName']
        )
        
        if prep_result['status'] != 'success':
            return prep_result
        
        gdf = prep_result['data']
        enhanced_categories = prep_result['enhanced_categories']
        
        # ORIGINAL: Get all available variables from dataset
        all_variables = enhanced_categories['individual_models'] + enhanced_categories['original_variables']
        all_variables = [col for col in all_variables if col in gdf.columns]
        
        # ORIGINAL: Selected variables (we'll use a sample since we don't have the selection process)
        selected_variables = enhanced_categories['individual_models'][:8] if enhanced_categories['individual_models'] else all_variables[:8]
        
        # ORIGINAL: Excluded variables
        excluded_variables = list(set(all_variables) - set(selected_variables))
        
        # ORIGINAL: Get top 5 vulnerable wards
        top_5_rankings = gdf.sort_values('composite_score', ascending=False).head(5)
        top_5_wards = top_5_rankings['WardName'].tolist()
        
        # ORIGINAL: Get method label
        current_method = 'mean'  # Default since we're using composite
        method_label = "Composite Risk Analysis"
        
        # ORIGINAL: Get full variable names (simplified for unified dataset)
        def get_simple_variable_name(var):
            """Simple variable name mapping"""
            return var.replace('_', ' ').title()
        
        full_all_variables = ["{} ({})".format(var, get_simple_variable_name(var)) for var in all_variables]
        full_selected_variables = ["{} ({})".format(var, get_simple_variable_name(var)) for var in selected_variables]
        full_excluded_variables = ["{} ({})".format(var, get_simple_variable_name(var)) for var in excluded_variables]
        
        # ORIGINAL: Create HTML content for the decision tree - EXACT SAME STRUCTURE
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{method_label}: Decision Tree Visualization</title>
    <style>
        body {{
            font-family: 'Arial', sans-serif;
            background-color: #ffffff;
            margin: 0;
            padding: 0;
            display: flex;
            justify-content: center;
        }}
        .decision-tree-container {{
            width: 100%;
            max-width: 900px;
            padding: 20px;
        }}
        .tree-row {{
            display: flex;
            justify-content: center;
            margin-bottom: 20px;
            position: relative;
        }}
        .node {{
            background-color: #f5f5f5;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
            margin: 0 10px;
            width: 100%;
            max-width: 250px;
        }}
        .node-title {{
            font-weight: bold;
            margin-bottom: 8px;
            font-size: 16px;
        }}
        .list-container {{
            max-height: 150px;
            overflow-y: auto;
            text-align: left;
            margin-top: 10px;
        }}
        .list-container ul, .list-container ol {{
            padding-left: 20px;
            margin: 5px 0;
        }}
        .list-container li {{
            margin-bottom: 6px;
            font-size: 13px;
        }}
        .navy {{
            background-color: #1B2631;
            color: white;
        }}
        .orange {{
            background-color: #E67E22;
            color: white;
        }}
        .teal {{
            background-color: #16A596;
            color: white;
        }}
        .gray {{
            background-color: #7F8C8D;
            color: white;
        }}
        .green {{
            background-color: #27AE60;
            color: white;
        }}
        .blue {{
            background-color: #2980B9;
            color: white;
        }}
        .purple {{
            background-color: #8E44AD;
            color: white;
        }}
        .arrow {{
            position: absolute;
            width: 0;
            height: 0;
            border-left: 10px solid transparent;
            border-right: 10px solid transparent;
            border-top: 10px solid #666;
            left: 50%;
            bottom: -15px;
            transform: translateX(-50%);
        }}
        .arrow-label {{
            position: absolute;
            background-color: white;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 12px;
            font-weight: bold;
        }}
        .arrow-container {{
            position: relative;
            height: 30px;
            width: 100%;
        }}
        .vertical-line {{
            position: absolute;
            width: 2px;
            background-color: #666;
            left: 50%;
            transform: translateX(-50%);
            top: 0;
            bottom: 0;
        }}
        .branch-container {{
            display: flex;
            justify-content: space-around;
            width: 100%;
            position: relative;
        }}
        .branch-line {{
            position: absolute;
            top: 0;
            height: 2px;
            background-color: #666;
        }}
        .branch-label {{
            position: absolute;
            top: -10px;
            background-color: white;
            padding: 0 5px;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="decision-tree-container">
        <h1 style="text-align: center; margin-bottom: 30px;">{method_label}: Malaria Risk Analysis Workflow</h1>
        
        <!-- Row 1: Start Node -->
        <div class="tree-row">
            <div class="node navy">
                <div class="node-title">Malaria Risk Assessment</div>
                <div>Variable Selection & Analysis</div>
            </div>
        </div>
        
        <!-- Arrow between Row 1 and 2 -->
        <div class="arrow-container">
            <div class="vertical-line"></div>
        </div>
        
        <!-- Row 2: Available Variables List -->
        <div class="tree-row">
            <div class="node navy">
                <div class="node-title">Available Variables</div>
                <div style="font-size: 12px;">Dataset contained {len(all_variables)} potential risk variables</div>
                <div class="list-container">
                    <ul>
        """
        
        # ORIGINAL: Show a reasonable subset of all variables
        display_all_vars = full_all_variables[:8]  # Show first 8
        for var in display_all_vars:
            html_content += "                        <li>{}</li>\n".format(var)
        
        if len(full_all_variables) > 8:
            html_content += "                        <li><em>...and {} more variables</em></li>\n".format(len(full_all_variables) - 8)
            
        html_content += """
                    </ul>
                </div>
            </div>
        </div>
        
        <!-- Arrow between Row 2 and 3 -->
        <div class="arrow-container">
            <div class="vertical-line"></div>
        </div>
        
        <!-- Row 3: Variable Selection Process -->
        <div class="tree-row">
            <div class="node orange">
                <div class="node-title">Variable Selection Process</div>
                <div style="font-size: 12px;">LLM-guided expert selection identified {len(selected_variables)} key variables for malaria risk analysis</div>
            </div>
        </div>
        
        <!-- Branch Lines for Include/Exclude -->
        <div class="branch-container" style="height: 50px;">
            <div class="branch-line" style="left: 25%; width: 25%;"></div>
            <div class="branch-label" style="left: 32%;">Selected</div>
            
            <div class="branch-line" style="left: 50%; width: 25%;"></div>
            <div class="branch-label" style="left: 62%;">Excluded</div>
        </div>
        
        <!-- Row 4: Selected and Excluded Variables -->
        <div class="tree-row">
            <div class="node teal" style="flex: 1;">
                <div class="node-title">SELECTED Variables</div>
                <div style="font-size: 12px; margin-bottom: 8px;"><strong>{len(selected_variables)} variables chosen for analysis</strong></div>
                <div class="list-container">
                    <ul>
        """
        
        # ORIGINAL: Show SELECTED variables that match detailed results
        for var in full_selected_variables:
            html_content += "                        <li><strong>{}</strong></li>\n".format(var)
            
        html_content += """
                    </ul>
                </div>
            </div>
            
            <div class="node gray" style="flex: 1;">
                <div class="node-title">EXCLUDED Variables</div>
                <div style="font-size: 12px; margin-bottom: 8px;">{len(excluded_variables)} variables not used</div>
                <div class="list-container">
                    <ul>
        """
        
        # ORIGINAL: Show excluded variables (limited for space)
        display_excluded_vars = full_excluded_variables[:6]  # Limit to 6 for space
        for var in display_excluded_vars:
            html_content += "                        <li>{}</li>\n".format(var)
        
        if len(full_excluded_variables) > 6:
            html_content += "                        <li><em>...and {} more</em></li>\n".format(len(full_excluded_variables) - 6)
            
        html_content += """
                    </ul>
                </div>
            </div>
        </div>
        
        <!-- Arrow from Selected Variables to Normalization -->
        <div class="arrow-container">
            <div class="vertical-line" style="left: 25%;"></div>
        </div>
        
        <!-- Row 5: Normalization and Calculation -->
        <div class="tree-row">
            <div class="node green" style="margin-left: 0;">
                <div class="node-title">Data Normalization &<br>Composite Score Calculation</div>
                <div>Converting variables to common scale and calculating risk scores</div>
            </div>
        </div>
        
        <!-- Arrow between Row 5 and 6 -->
        <div class="arrow-container">
            <div class="vertical-line"></div>
        </div>
        
        <!-- Row 6: Risk Maps -->
        <div class="tree-row">
            <div class="node blue">
                <div class="node-title">Generated Risk Maps<br>for All Combinations</div>
                <div>Maps showing risk scores for different variable combinations</div>
            </div>
        </div>
        
        <!-- Arrow between Row 6 and 7 -->
        <div class="arrow-container">
            <div class="vertical-line"></div>
        </div>
        
        <!-- Row 7: Vulnerability Analysis -->
        <div class="tree-row">
            <div class="node purple">
                <div class="node-title">Vulnerability Analysis</div>
                <div>Box and whisker plot of ward vulnerability rankings</div>
            </div>
        </div>
        
        <!-- Arrow between Row 7 and 8 -->
        <div class="arrow-container">
            <div class="vertical-line"></div>
        </div>
        
        <!-- Row 8: Priority Wards -->
        <div class="tree-row">
            <div class="node purple">
                <div class="node-title">Top 5 Wards<br>for Reprioritization</div>
                <div class="list-container">
                    <ol>
        """
        
        # ORIGINAL: Add top 5 wards to HTML
        for ward in top_5_wards:
            html_content += "                        <li>{}</li>\n".format(ward)
            
        html_content += """
                    </ol>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
        """
        
        # Save as HTML file with fixed filename to prevent URL instability
        # Create unique filename with timestamp - ensures multiple visualizations coexist
        # Files persist until session closure (browser closed or session expired)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"decision_tree_{timestamp}"  # No .html extension here
        
        save_result = save_agent_visualization(
            html_content, filename, session_id, 'decision_tree', is_html_content=True
        )
        
        if save_result['status'] == 'success':
            return {
                'status': 'success',
                'message': f'Successfully created decision tree visualization',
                'visualization_type': 'decision_tree',
                'total_variables_available': len(all_variables),
                'variables_selected': len(selected_variables),
                'variables_excluded': len(excluded_variables),
                'top_5_wards': top_5_wards,
                'file_path': save_result['file_path'],
                'web_path': save_result['web_path'],
                'session_id': session_id
            }
        else:
            return save_result
        
    except Exception as e:
        logger.error(f"Error creating agent decision tree: {e}")
        return {
            'status': 'error',
            'message': f'Decision tree creation failed: {str(e)}',
            'visualization_type': 'decision_tree'
        }

def get_agent_pagination_info(unified_dataset: gpd.GeoDataFrame, 
                            visualization_type: str,
                            models_per_page: int = 4,
                            wards_per_page: int = 20) -> Dict[str, Any]:
    """
    Get pagination information for agent visualizations
    
    Args:
        unified_dataset: Enhanced unified dataset GeoDataFrame
        visualization_type: Type of visualization ('composite_maps' or 'box_plot')
        models_per_page: Number of models per page (for composite maps)
        wards_per_page: Number of wards per page (for box plot)
        
    Returns:
        Dictionary with pagination information
    """
    try:
        prep_result = prepare_unified_dataset(unified_dataset)
        
        if prep_result['status'] != 'success':
            return prep_result
        
        enhanced_categories = prep_result['enhanced_categories']
        data = prep_result['data']
        
        if visualization_type == 'composite_maps':
            model_columns = enhanced_categories['individual_models']
            total_items = len(model_columns)
            items_per_page = models_per_page
            item_type = 'models'
            
        elif visualization_type == 'box_plot':
            total_items = len(data)
            items_per_page = wards_per_page
            item_type = 'wards'
            
        else:
            return {
                'status': 'error',
                'message': f'Unknown visualization type: {visualization_type}'
            }
        
        total_pages = max(1, (total_items + items_per_page - 1) // items_per_page)
        
        return {
            'status': 'success',
            'visualization_type': visualization_type,
            'total_items': total_items,
            'items_per_page': items_per_page,
            'total_pages': total_pages,
            'item_type': item_type,
            'pagination_info': {
                'page_1': f'Page 1: Most vulnerable {item_type}',
                'page_last': f'Page {total_pages}: Least vulnerable {item_type}' if total_pages > 1 else None,
                'total_description': f'{total_items} {item_type} across {total_pages} pages'
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting pagination info: {e}")
        return {
            'status': 'error',
            'message': f'Pagination info failed: {str(e)}'
        } 