"""
Visualization themes and color schemes

This module provides consistent color schemes and theming utilities
for all visualization components.
"""

import logging
import plotly.graph_objects as go
import plotly.io as pio

# Set up logging
logger = logging.getLogger(__name__)

# Create a theme matching the original ChatMRPT-main styling
# These are the exact colors and settings from the original
THEME_COLORS = {
    'primary': '#3c5e8b',  # Primary blue
    'secondary': '#69b3a2',  # Green-blue
    'accent': '#f8ac61',    # Orange accent
    'highlight': '#e63946', # Red highlight
    'background': '#F8F9FA', # Light gray background
    'text': '#333333',      # Dark text
    
    # Box plot category colors
    'high_risk': '#69b3a2',   # Green-blue for high risk
    'medium_risk': '#a8d8b9', # Light green for medium risk 
    'low_risk': '#c7e9c0',    # Very light green for low risk
    
    # Map colors
    'map_outline': 'black',
    'non_urban_outline': 'blue',
    
    # Choropleth colorscales
    'risk_colorscale': 'YlOrRd',  # Yellow-Orange-Red for risk maps
    'variable_colorscale': 'Blues', # Blues for variable maps
}

# Font settings
THEME_FONTS = {
    'title': {
        'family': 'Arial, sans-serif',
        'size': 20,
        'color': THEME_COLORS['text']
    },
    'subtitle': {
        'family': 'Arial, sans-serif',
        'size': 16,
        'color': THEME_COLORS['text']
    },
    'axis': {
        'family': 'Arial, sans-serif',
        'size': 14,
        'color': THEME_COLORS['text']
    },
    'annotation': {
        'family': 'Arial, sans-serif',
        'size': 14,
        'color': 'darkred'
    }
}

# Layout settings
THEME_LAYOUT = {
    'plot_bgcolor': THEME_COLORS['background'],
    'paper_bgcolor': THEME_COLORS['background'],
    'colorway': [THEME_COLORS['primary'], THEME_COLORS['secondary'], THEME_COLORS['accent']],
    'template': 'plotly_white'
}

# Risk score labels (for maps)
RISK_LABELS = ["Very Low", "Low", "Medium", "High", "Very High"]
RISK_VALUES = [0, 0.25, 0.5, 0.75, 1]

def get_color_scheme(viz_type):
    """Get appropriate color scheme for visualization type"""
    if viz_type in ['composite_map', 'vulnerability_map']:
        return THEME_COLORS['risk_colorscale']
    elif viz_type in ['variable_map', 'normalized_map']:
        return THEME_COLORS['variable_colorscale']
    else:
        return THEME_COLORS['risk_colorscale']  # Default

def get_risk_labels():
    """Get risk labels and values for color scales"""
    return {
        'tickvals': RISK_VALUES,
        'ticktext': RISK_LABELS
    }

def get_vulnerability_colors():
    """Get colors for vulnerability categories"""
    return {
        'High': THEME_COLORS['high_risk'],
        'Medium': THEME_COLORS['medium_risk'],
        'Low': THEME_COLORS['low_risk']
    }

def get_map_styling():
    """Get consistent map styling settings"""
    return {
        'mapbox_style': 'carto-positron',
        'marker_line_color': THEME_COLORS['map_outline'],
        'marker_line_width': 0.5,
        'non_urban_line_color': THEME_COLORS['non_urban_outline'],
        'non_urban_line_width': 3
    }

def get_chart_styling():
    """Get consistent chart styling settings"""
    return {
        'boxplot': {
            'line_color': THEME_COLORS['primary'],
            'line_width': 1.5,
            'show_mean': True
        },
        'margin': {
            'standard': dict(l=150, r=20, t=80, b=50),
            'map': dict(l=20, r=20, t=100, b=20)
        },
        'height': {
            'boxplot': 520,
            'map': 700
        }
    }

def create_custom_colorscale(low_color, high_color, n_steps=10):
    """Create a custom colorscale between two colors"""
    import numpy as np
    import matplotlib.colors as mcolors
    
    # Create array of n_steps evenly spaced between 0 and 1
    scale = np.linspace(0, 1, n_steps)
    
    # Create list of (value, color) tuples
    colorscale = []
    for i, value in enumerate(scale):
        # Interpolate between low_color and high_color
        r = mcolors.rgb_to_hsv(mcolors.to_rgb(low_color))
        g = mcolors.rgb_to_hsv(mcolors.to_rgb(high_color))
        rgb = mcolors.hsv_to_rgb((r[0] * (1 - value) + g[0] * value, 
                                 r[1] * (1 - value) + g[1] * value,
                                 r[2] * (1 - value) + g[2] * value))
        hex_color = mcolors.to_hex(rgb)
        colorscale.append([value, hex_color])
    
    return colorscale

def apply_theme_to_figure(fig, viz_type):
    """Apply consistent theming to a plotly figure"""
    # Apply common layout settings
    fig.update_layout(
        font=THEME_FONTS['axis'],
        plot_bgcolor=THEME_LAYOUT['plot_bgcolor'],
        paper_bgcolor=THEME_LAYOUT['paper_bgcolor'],
        template=THEME_LAYOUT['template']
    )
    
    # Apply specific settings based on visualization type
    if viz_type in ['boxplot', 'vulnerability_plot']:
        fig.update_layout(
            height=get_chart_styling()['height']['boxplot'],
            margin=get_chart_styling()['margin']['standard']
        )
    elif viz_type in ['composite_map', 'variable_map', 'vulnerability_map']:
        fig.update_layout(
            height=get_chart_styling()['height']['map'],
            margin=get_chart_styling()['margin']['map']
        )
    
    return fig

def get_theme_summary():
    """Get a summary of the current theme settings"""
    return {
        'colors': THEME_COLORS,
        'fonts': THEME_FONTS,
        'layout': THEME_LAYOUT
    } 