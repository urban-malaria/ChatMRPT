# app/visualization/charts.py
import os
import json
import logging
import concurrent.futures
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from flask import session, current_app

# Import from other visualization modules
from .core import get_full_variable_name, is_id_column
from .export import create_plotly_html
from .themes import get_chart_styling, get_vulnerability_colors, apply_theme_to_figure

# Set up logging
logger = logging.getLogger(__name__)


def box_plot_function(processed_scores, wards_per_page=20):
    """
    Create paginated box plots of ward rankings
    
    Args:
        processed_scores: DataFrame with processed model scores data
        wards_per_page: Number of wards to display per page (default: 20)
        
    Returns:
        Dict with plotly objects for each page and ward rankings
    """
    try:
        # Create a copy to avoid modifying original
        df_long = processed_scores.copy()
        
        # Get model columns (starting with 'model_')
        model_cols = [col for col in df_long.columns if col.startswith('model_')]
        
        if not model_cols:
            return {
                'status': 'error',
                'message': 'No model scores found in data'
            }
        
        # Melt the dataframe to long format for plotting - vectorized operation
        melted_df = pd.melt(
            df_long, 
            id_vars=['WardName'], 
            value_vars=model_cols,
            var_name='variable', 
            value_name='value'
        )
        
        # Calculate ward rankings - lower rank value = HIGHER vulnerability
        # Vectorized operations using pandas
        ward_rankings = melted_df.groupby('WardName')['value'].median().reset_index()
        ward_rankings = ward_rankings.sort_values('value', ascending=False)
        ward_rankings['overall_rank'] = range(1, len(ward_rankings) + 1)
        
        # Create vulnerability categories (high, medium, low)
        ward_rankings['vulnerability_category'] = pd.cut(
            ward_rankings['overall_rank'],
            bins=[0, len(ward_rankings)//3, 2*len(ward_rankings)//3, len(ward_rankings)],
            labels=['High', 'Medium', 'Low']
        )
        
        # Merge rankings back to the melted dataframe - vectorized
        df_long = pd.merge(melted_df, ward_rankings[['WardName', 'overall_rank', 'vulnerability_category']], on='WardName')
        
        # Sort by overall rank (most vulnerable wards at the top)
        df_long['WardName'] = pd.Categorical(
            df_long['WardName'],
            categories=ward_rankings.sort_values('overall_rank')['WardName'],
            ordered=True
        )
        
        # Calculate the number of pages needed
        total_wards = len(ward_rankings)
        total_pages = (total_wards + wards_per_page - 1) // wards_per_page
        
        # Store page data for later reference
        page_data = {}
        
        # Define a function to create a plot for a single page
        def create_page_plot(page):
            # Calculate start and end indices for this page
            start_idx = (page - 1) * wards_per_page
            end_idx = min(start_idx + wards_per_page, total_wards)
            
            # Get ward names for this page based on ranking
            page_wards = ward_rankings.sort_values('overall_rank')['WardName'].iloc[start_idx:end_idx].tolist()
            
            # Filter data for these wards - vectorized
            page_data[str(page)] = []
            for ward in page_wards:
                ward_rank_info = ward_rankings[ward_rankings['WardName'] == ward].iloc[0]
                page_data[str(page)].append({
                    'ward_name': ward,
                    'overall_rank': int(ward_rank_info['overall_rank']),
                    'median_score': float(ward_rank_info['value']),
                    'vulnerability_category': str(ward_rank_info['vulnerability_category'])
                })
            
            page_df = df_long[df_long['WardName'].isin(page_wards)].copy()
            
            # Create helper column for sorting
            page_df = pd.merge(
                page_df,
                pd.DataFrame({'WardName': page_wards, 'sort_order': range(len(page_wards))}),
                on='WardName'
            )
            
            # Sort by the helper column
            page_df = page_df.sort_values('sort_order')
            
            # Create figure
            fig = go.Figure()
            
            # For each ward, add a box plot
            for ward in page_wards:
                ward_data = page_df[page_df['WardName'] == ward]
                rank = ward_rankings[ward_rankings['WardName'] == ward]['overall_rank'].values[0]
                category = ward_rankings[ward_rankings['WardName'] == ward]['vulnerability_category'].values[0]
                
                # Set color based on vulnerability category - using the theme module
                vuln_colors = get_vulnerability_colors()
                box_color = vuln_colors[category]
                
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
            
            # Get styling from the theme module
            chart_styling = get_chart_styling()
            
            # Update layout - EXACTLY MATCH ORIGINAL STYLING
            fig.update_layout(
                title={
                    'text': f'Ward Rankings Distribution (Page {page} of {total_pages})',
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
                    'showgrid': True,
                    'range': [0, 1]  # Ensure consistent range across all pages
                },
                yaxis={
                    'title': '',
                    'categoryorder': 'array',
                    'categoryarray': page_wards,
                    'gridcolor': '#E5E5E5',
                    'showgrid': True
                },
                height=chart_styling['height']['boxplot'],
                margin=chart_styling['margin']['standard'],
                annotations=[
                    dict(
                        x=0.5, y=-0.15,
                        text="Most vulnerable wards at top | Least vulnerable at bottom",
                        showarrow=False,
                        xref="paper", yref="paper",
                        font=dict(size=14, color='darkred')
                    )
                ],
                autosize=True,
            )
            
            # Apply theme to figure for consistent styling
            fig = apply_theme_to_figure(fig, 'boxplot')
            
            return fig
        
        # Use parallelization to create plots for all pages - this is a heavy operation
        # For smaller datasets or fewer pages, we can use a simple loop
        if total_pages <= 5:
            plot_list = [create_page_plot(page) for page in range(1, total_pages + 1)]
        else:
            # Use parallel processing for larger datasets with many pages
            with concurrent.futures.ThreadPoolExecutor() as executor:
                plot_list = list(executor.map(create_page_plot, range(1, total_pages + 1)))
        
        # Return the results as a dictionary
        return {
            'status': 'success',
            'message': 'Successfully created vulnerability box plots',
            'plots': plot_list, 
            'ward_rankings': ward_rankings,
            'total_pages': total_pages,
            'current_page': 1,
            'page_data': page_data
        }
    
    except Exception as e:
        logger.error("Error creating vulnerability plot: {}".format(str(e)), exc_info=True)
        import traceback
        logger.error(traceback.format_exc())
        return {
            'status': 'error',
            'message': 'Error creating vulnerability plot: {}'.format(str(e))
        }


def create_vulnerability_plot(data_handler):
    """
    Create a box and whisker plot visualization of ward vulnerability rankings
    
    Args:
        data_handler: DataHandler instance with composite scores
        
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
        
        # Generate box plots
        box_plot_result = box_plot_function(data_handler.composite_scores['scores'])
        
        if box_plot_result['status'] == 'success':
            # Store the box plot data for pagination
            data_handler.boxwhisker_plot = box_plot_result
            
            # Get the first plot
            plot_fig = box_plot_result['plots'][0]
            
            # Save as HTML
            html_path = create_plotly_html(plot_fig, "vulnerability_plot.html")
            
            result = {
                'status': 'success',
                'message': 'Successfully created vulnerability box plot',
                'image_path': html_path,
                'current_page': 1,
                'total_pages': box_plot_result['total_pages'],
                'viz_type': 'vulnerability_plot',
                'data_summary': {
                    'ward_count': len(box_plot_result['ward_rankings']),
                    'high_vulnerability_count': len(box_plot_result['ward_rankings'][box_plot_result['ward_rankings']['vulnerability_category'] == 'High']),
                    'medium_vulnerability_count': len(box_plot_result['ward_rankings'][box_plot_result['ward_rankings']['vulnerability_category'] == 'Medium']),
                    'low_vulnerability_count': len(box_plot_result['ward_rankings'][box_plot_result['ward_rankings']['vulnerability_category'] == 'Low'])
                },
                'visual_elements': {
                    'plot_type': 'Box and whisker plot',
                    'color_scheme': 'By vulnerability category',
                    'axis_meanings': {
                        'x': 'Risk Score (0-1 scale)',
                        'y': 'Ward Names (ordered by vulnerability rank)'
                    }
                }
            }
            
            return result
        else:
            return box_plot_result
            
    except Exception as e:
        logger.error("Error creating vulnerability plot: {}".format(str(e)), exc_info=True)
        return {
            'status': 'error',
            'message': 'Error creating vulnerability plot: {}'.format(str(e))
        }


def create_decision_tree_plot(data_handler):
    """
    Create a decision tree visualization flowing from top to bottom
    
    Args:
        data_handler: DataHandler instance
        
    Returns:
        dict: Status and visualization information
    """
    try:
        # Get all variables and selected variables
        all_variables = []
        selected_variables = []
        excluded_variables = []
        top_5_wards = []
        
        # Get all variables from original data - use vectorized operations
        if data_handler.csv_data is not None:
            all_variables = [col for col in data_handler.csv_data.columns 
                           if col != 'WardName' and pd.api.types.is_numeric_dtype(data_handler.csv_data[col]) and not is_id_column(col)]
        
        # Get selected variables from composite scores
        if hasattr(data_handler, 'composite_variables') and data_handler.composite_variables:
            selected_variables = data_handler.composite_variables
        elif data_handler.composite_scores is not None and 'formulas' in data_handler.composite_scores:
            # Use variables from the first model
            if data_handler.composite_scores['formulas']:
                selected_variables = data_handler.composite_scores['formulas'][0]['variables']
                # Clean up variable names if needed
                selected_variables = [var.replace('normalization_', '') for var in selected_variables]
        
        # Get excluded variables - vectorized set operation
        excluded_variables = list(set(all_variables) - set(selected_variables))
        
        # Get top 5 vulnerable wards
        if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
            top_5 = data_handler.vulnerability_rankings.sort_values('overall_rank').head(5)
            top_5_wards = top_5['WardName'].tolist()
        
        # Get full variable names
        full_all_variables = ["{} ({})".format(var, get_full_variable_name(var)) for var in all_variables]
        full_selected_variables = ["{} ({})".format(var, get_full_variable_name(var)) for var in selected_variables]
        full_excluded_variables = ["{} ({})".format(var, get_full_variable_name(var)) for var in excluded_variables]
        
        # Create HTML content for the decision tree
        html_content = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Decision Tree Visualization</title>
    <style>
        body {
            font-family: 'Arial', sans-serif;
            background-color: #ffffff;
            margin: 0;
            padding: 0;
            display: flex;
            justify-content: center;
        }
        .decision-tree-container {
            width: 100%;
            max-width: 900px;
            padding: 20px;
        }
        .tree-row {
            display: flex;
            justify-content: center;
            margin-bottom: 20px;
            position: relative;
        }
        .node {
            background-color: #f5f5f5;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
            margin: 0 10px;
            width: 100%;
            max-width: 250px;
        }
        .node-title {
            font-weight: bold;
            margin-bottom: 8px;
            font-size: 16px;
        }
        .list-container {
            max-height: 150px;
            overflow-y: auto;
            text-align: left;
            margin-top: 10px;
        }
        .list-container ul, .list-container ol {
            padding-left: 20px;
            margin: 5px 0;
        }
        .list-container li {
            margin-bottom: 6px;
            font-size: 13px;
        }
        .navy {
            background-color: #1B2631;
            color: white;
        }
        .orange {
            background-color: #E67E22;
            color: white;
        }
        .teal {
            background-color: #16A596;
            color: white;
        }
        .gray {
            background-color: #7F8C8D;
            color: white;
        }
        .green {
            background-color: #27AE60;
            color: white;
        }
        .blue {
            background-color: #2980B9;
            color: white;
        }
        .purple {
            background-color: #8E44AD;
            color: white;
        }
        .arrow {
            position: absolute;
            width: 0;
            height: 0;
            border-left: 10px solid transparent;
            border-right: 10px solid transparent;
            border-top: 10px solid #666;
            left: 50%;
            bottom: -15px;
            transform: translateX(-50%);
        }
        .arrow-label {
            position: absolute;
            background-color: white;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 12px;
            font-weight: bold;
        }
        .arrow-container {
            position: relative;
            height: 30px;
            width: 100%;
        }
        .vertical-line {
            position: absolute;
            width: 2px;
            background-color: #666;
            left: 50%;
            transform: translateX(-50%);
            top: 0;
            bottom: 0;
        }
        .branch-container {
            display: flex;
            justify-content: space-around;
            width: 100%;
            position: relative;
        }
        .branch-line {
            position: absolute;
            top: 0;
            height: 2px;
            background-color: #666;
        }
        .branch-label {
            position: absolute;
            top: -10px;
            background-color: white;
            padding: 0 5px;
            font-size: 12px;
        }
    </style>
</head>
<body>
    <div class="decision-tree-container">
        <h1 style="text-align: center; margin-bottom: 30px;">Malaria Risk Analysis Workflow</h1>
        
        <!-- Row 1: Start Node -->
        <div class="tree-row">
            <div class="node navy">
                <div class="node-title">Malaria Risk Assessment</div>
                <div>Variable Selection</div>
            </div>
        </div>
        
        <!-- Arrow between Row 1 and 2 -->
        <div class="arrow-container">
            <div class="vertical-line"></div>
        </div>
        
        <!-- Row 2: Variables List -->
        <div class="tree-row">
            <div class="node navy">
                <div class="node-title">Variables</div>
                <div class="list-container">
                    <ul>
        """
        
        # Add all variables to HTML
        for var in full_all_variables[:10]:  # Limit to first 10 for space
            html_content += "                        <li>{}</li>\n".format(var)
        
        if len(full_all_variables) > 10:
            html_content += "                        <li>...and {} more</li>\n".format(len(full_all_variables) - 10)
            
        html_content += """
                    </ul>
                </div>
            </div>
        </div>
        
        <!-- Arrow between Row 2 and 3 -->
        <div class="arrow-container">
            <div class="vertical-line"></div>
        </div>
        
        <!-- Row 3: Evaluation Diamond -->
        <div class="tree-row">
            <div class="node orange">
                <div class="node-title">Variable Evaluation</div>
                <div>Assessment of variable relationships with malaria risk</div>
            </div>
        </div>
        
        <!-- Branch Lines for Include/Exclude -->
        <div class="branch-container" style="height: 50px;">
            <div class="branch-line" style="left: 25%; width: 25%;"></div>
            <div class="branch-label" style="left: 32%;">Include</div>
            
            <div class="branch-line" style="left: 50%; width: 25%;"></div>
            <div class="branch-label" style="left: 62%;">Exclude</div>
        </div>
        
        <!-- Row 4: Included and Excluded Variables -->
        <div class="tree-row">
            <div class="node teal" style="flex: 1;">
                <div class="node-title">Included Variables</div>
                <div class="list-container">
                    <ul>
        """
        
        # Add included variables to HTML
        for var in full_selected_variables:
            html_content += "                        <li>{}</li>\n".format(var)
        
        if not full_selected_variables:
            html_content += "                        <li>No variables selected yet</li>\n"
            
        html_content += """
                    </ul>
                </div>
            </div>
            
            <div class="node gray" style="flex: 1;">
                <div class="node-title">Excluded Variables</div>
                <div class="list-container">
                    <ul>
        """
        
        # Add excluded variables to HTML
        for var in full_excluded_variables[:10]:  # Limit to first 10 for space
            html_content += "                        <li>{}</li>\n".format(var)
        
        if len(full_excluded_variables) > 10:
            html_content += "                        <li>...and {} more</li>\n".format(len(full_excluded_variables) - 10)
        
        if not full_excluded_variables:
            html_content += "                        <li>No variables excluded yet</li>\n"
            
        html_content += """
                    </ul>
                </div>
            </div>
        </div>
        
        <!-- Arrow from Included Variables to Normalization -->
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
        
        # Add top 5 wards to HTML
        for ward in top_5_wards:
            html_content += "                        <li>{}</li>\n".format(ward)
        
        if not top_5_wards:
            html_content += "                        <li>No wards ranked yet</li>\n"
            
        html_content += """
                    </ol>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
        """
        
        # Save HTML to a file
        session_id = session.get('session_id', 'default')
        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
        os.makedirs(session_folder, exist_ok=True)
        
        file_path = os.path.join(session_folder, 'decision_tree.html')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        web_path = "/serve_viz_file/{}/decision_tree.html".format(session_id)
        
        # Create rich context for LLM
        data_summary = {
            'all_variables_count': len(all_variables),
            'selected_variables_count': len(selected_variables),
            'excluded_variables_count': len(excluded_variables),
            'top_5_wards': top_5_wards,
            'selected_variables': selected_variables,
            'full_selected_variables': full_selected_variables
        }
        
        visual_elements = {
            'visualization_type': 'decision_tree',
            'color_scheme': 'Multiple colors for different stages',
            'flow_direction': 'Top to bottom',
            'interactive_elements': 'Scrollable variable lists',
            'node_count': 8
        }
        
        # Return success with paths and metadata
        return {
            'status': 'success',
            'message': 'Successfully created decision tree visualization',
            'image_path': web_path,
            'viz_type': 'decision_tree',
            'data_summary': data_summary,
            'visual_elements': visual_elements
        }
        
    except Exception as e:
        logger.error("Error creating decision tree plot: {}".format(str(e)), exc_info=True)
        import traceback
        logger.error(traceback.format_exc())
        return {
            'status': 'error',
            'message': 'Error creating decision tree plot: {}'.format(str(e))
        }


def get_available_chart_types():
    """
    Get list of available chart visualization types
    
    Returns:
        list: Available chart types with descriptions
    """
    return [
        {
            'type': 'vulnerability_plot',
            'name': 'Vulnerability Box Plot',
            'description': 'Box and whisker plot showing ward vulnerability rankings',
            'requires_analysis': True
        },
        {
            'type': 'decision_tree',
            'name': 'Analysis Decision Tree',
            'description': 'Flowchart showing the analysis workflow and decisions',
            'requires_analysis': False
        }
    ]


def validate_chart_inputs(data_handler, chart_type, **kwargs):
    """
    Validate inputs for chart creation
    
    Args:
        data_handler: DataHandler instance
        chart_type: Type of chart to create
        **kwargs: Additional parameters
        
    Returns:
        dict: Validation result
    """
    if data_handler is None:
        return {
            'status': 'error',
            'message': 'Data handler is required'
        }
    
    # Type-specific validation
    if chart_type == 'vulnerability_plot':
        if not hasattr(data_handler, 'composite_scores') or data_handler.composite_scores is None:
            return {
                'status': 'error',
                'message': 'Analysis must be completed before creating vulnerability plots'
            }
    
    elif chart_type == 'decision_tree':
        if data_handler.csv_data is None:
            return {
                'status': 'error',
                'message': 'CSV data is required for decision tree visualization'
            }
    
    return {
        'status': 'success',
        'message': 'Validation passed'
    }


def get_chart_summary(data_handler):
    """
    Get a summary of available chart visualizations
    
    Args:
        data_handler: DataHandler instance
        
    Returns:
        dict: Summary of available charts and data status
    """
    available_charts = []
    
    # Check what charts can be created
    chart_types = get_available_chart_types()
    
    for chart_info in chart_types:
        validation = validate_chart_inputs(data_handler, chart_info['type'])
        chart_info['available'] = validation['status'] == 'success'
        chart_info['requirement_message'] = validation.get('message', '')
        available_charts.append(chart_info)
    
    # Handle None data_handler safely
    has_csv_data = data_handler is not None and hasattr(data_handler, 'csv_data') and data_handler.csv_data is not None
    analysis_complete = data_handler is not None and hasattr(data_handler, 'composite_scores') and data_handler.composite_scores is not None
    
    return {
        'status': 'success',
        'available_charts': available_charts,
        'has_csv_data': has_csv_data,
        'analysis_complete': analysis_complete
    } 