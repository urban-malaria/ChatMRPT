"""
SAMPLE IMPLEMENTATION: High-Priority Statistical & Visualization Enhancements
This demonstrates how to implement the most impactful improvements to ChatMRPT.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from sklearn.ensemble import RandomForestRegressor
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)

# ========================================
# 1. INTELLIGENT VARIABLE SELECTION
# ========================================

def intelligent_variable_selection(session_id: str, target_variable: str = None, 
                                 max_features: int = 10) -> Dict[str, Any]:
    """
    Automatically select optimal variables for analysis using multiple methods.
    
    HIGH PRIORITY ENHANCEMENT: Replaces manual variable selection with AI-powered optimization.
    """
    try:
        from app.tools.data_tools import _get_unified_dataset
        df = _get_unified_dataset(session_id)
        
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        # Get numeric columns only
        numeric_cols = list(df.select_dtypes(include=[np.number]).columns)
        
        # Remove ID columns and the target if specified
        exclude_terms = ['id', 'index', '_id', 'fid']
        if target_variable:
            exclude_terms.append(target_variable.lower())
            
        numeric_cols = [col for col in numeric_cols 
                       if not any(term in col.lower() for term in exclude_terms)]
        
        if len(numeric_cols) < 2:
            return {'status': 'error', 'message': 'Insufficient numeric variables for analysis'}
        
        # Method 1: Correlation-based selection
        corr_results = _correlation_based_selection(df[numeric_cols], max_features)
        
        # Method 2: Random Forest importance (if target variable provided)
        rf_results = None
        if target_variable and target_variable in df.columns:
            rf_results = _random_forest_selection(df, numeric_cols, target_variable, max_features)
        
        # Method 3: Variance-based selection
        variance_results = _variance_based_selection(df[numeric_cols], max_features)
        
        # Combine results intelligently
        final_selection = _combine_selection_methods(
            corr_results, rf_results, variance_results, max_features
        )
        
        return {
            'status': 'success',
            'message': f'Intelligent variable selection completed - {len(final_selection)} variables selected',
            'selected_variables': final_selection,
            'selection_methods': {
                'correlation_based': corr_results,
                'random_forest': rf_results,
                'variance_based': variance_results
            },
            'recommendation': f"Selected {len(final_selection)} optimal variables for analysis based on correlation, importance, and variance criteria."
        }
        
    except Exception as e:
        logger.error(f"Error in intelligent variable selection: {e}")
        return {'status': 'error', 'message': f'Selection failed: {str(e)}'}


def _correlation_based_selection(df: pd.DataFrame, max_features: int) -> List[str]:
    """Select variables with high correlation to others but low multicollinearity."""
    corr_matrix = df.corr().abs()
    
    # Calculate correlation strength (sum of correlations with other variables)
    corr_strength = corr_matrix.sum() - 1  # Subtract self-correlation
    
    # Remove highly collinear variables (correlation > 0.9)
    selected = []
    remaining = list(df.columns)
    
    # Sort by correlation strength
    strength_order = corr_strength.sort_values(ascending=False).index
    
    for var in strength_order:
        if var in remaining:
            selected.append(var)
            
            # Remove highly correlated variables
            highly_corr = corr_matrix[var][corr_matrix[var] > 0.9].index.tolist()
            for corr_var in highly_corr:
                if corr_var != var and corr_var in remaining:
                    remaining.remove(corr_var)
            
            if len(selected) >= max_features:
                break
    
    return selected


def _random_forest_selection(df: pd.DataFrame, feature_cols: List[str], 
                           target_col: str, max_features: int) -> List[str]:
    """Use Random Forest to rank variable importance."""
    # Prepare data
    X = df[feature_cols].fillna(df[feature_cols].median())
    y = df[target_col].fillna(df[target_col].median())
    
    # Fit Random Forest
    rf = RandomForestRegressor(n_estimators=100, random_state=42)
    rf.fit(X, y)
    
    # Get feature importance
    importance_df = pd.DataFrame({
        'variable': feature_cols,
        'importance': rf.feature_importances_
    }).sort_values('importance', ascending=False)
    
    return importance_df.head(max_features)['variable'].tolist()


def _variance_based_selection(df: pd.DataFrame, max_features: int) -> List[str]:
    """Select variables with highest variance (most informative)."""
    # Standardize data
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df.fillna(df.median()))
    
    # Calculate variance
    variances = np.var(scaled_data, axis=0)
    variance_df = pd.DataFrame({
        'variable': df.columns,
        'variance': variances
    }).sort_values('variance', ascending=False)
    
    return variance_df.head(max_features)['variable'].tolist()


def _combine_selection_methods(corr_results: List[str], rf_results: Optional[List[str]], 
                             var_results: List[str], max_features: int) -> List[str]:
    """Intelligently combine results from multiple selection methods."""
    # Score variables based on appearance in different methods
    variable_scores = {}
    
    # Correlation method (weight: 0.4)
    for i, var in enumerate(corr_results):
        variable_scores[var] = variable_scores.get(var, 0) + (0.4 * (len(corr_results) - i))
    
    # Random Forest method (weight: 0.4 if available, else redistribute)
    if rf_results:
        for i, var in enumerate(rf_results):
            variable_scores[var] = variable_scores.get(var, 0) + (0.4 * (len(rf_results) - i))
        variance_weight = 0.2
    else:
        variance_weight = 0.6
    
    # Variance method
    for i, var in enumerate(var_results):
        variable_scores[var] = variable_scores.get(var, 0) + (variance_weight * (len(var_results) - i))
    
    # Sort by combined score and return top variables
    sorted_vars = sorted(variable_scores.items(), key=lambda x: x[1], reverse=True)
    return [var for var, score in sorted_vars[:max_features]]


# ========================================
# 2. MACHINE LEARNING INTEGRATION
# ========================================

def machine_learning_analysis(session_id: str, analysis_type: str = 'clustering', 
                             n_clusters: int = 3) -> Dict[str, Any]:
    """
    Perform machine learning analysis on malaria data.
    
    HIGH PRIORITY ENHANCEMENT: Adds advanced ML capabilities.
    """
    try:
        from app.tools.data_tools import _get_unified_dataset
        df = _get_unified_dataset(session_id)
        
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        # Get optimal variables using intelligent selection
        var_selection = intelligent_variable_selection(session_id, max_features=8)
        if var_selection['status'] != 'success':
            return var_selection
        
        selected_vars = var_selection['selected_variables']
        
        if analysis_type == 'clustering':
            return _perform_clustering_analysis(df, selected_vars, n_clusters, session_id)
        elif analysis_type == 'importance_ranking':
            return _perform_importance_ranking(df, selected_vars, session_id)
        else:
            return {'status': 'error', 'message': f'Unknown analysis type: {analysis_type}'}
            
    except Exception as e:
        logger.error(f"Error in ML analysis: {e}")
        return {'status': 'error', 'message': f'ML analysis failed: {str(e)}'}


def _perform_clustering_analysis(df: pd.DataFrame, variables: List[str], 
                               n_clusters: int, session_id: str) -> Dict[str, Any]:
    """Perform K-means clustering analysis."""
    # Prepare data
    cluster_data = df[variables].fillna(df[variables].median())
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(cluster_data)
    
    # Perform clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    clusters = kmeans.fit_predict(scaled_data)
    
    # Calculate silhouette score
    silhouette = silhouette_score(scaled_data, clusters)
    
    # Add clusters to dataframe
    df_with_clusters = df.copy()
    df_with_clusters['cluster'] = clusters
    df_with_clusters['cluster_name'] = df_with_clusters['cluster'].map({
        0: 'High Risk', 1: 'Medium Risk', 2: 'Low Risk'
    })
    
    # Analyze cluster characteristics
    cluster_summary = df_with_clusters.groupby('cluster')[variables].mean()
    
    # Create visualization
    viz_result = _create_cluster_visualization(df_with_clusters, variables[0], variables[1], session_id)
    
    return {
        'status': 'success',
        'message': f'K-means clustering completed with {n_clusters} clusters (silhouette score: {silhouette:.3f})',
        'clusters': clusters.tolist(),
        'cluster_centers': kmeans.cluster_centers_.tolist(),
        'silhouette_score': silhouette,
        'cluster_summary': cluster_summary.to_dict(),
        'variables_used': variables,
        'visualization': viz_result
    }


def _perform_importance_ranking(df: pd.DataFrame, variables: List[str], session_id: str) -> Dict[str, Any]:
    """Perform Random Forest variable importance ranking."""
    # Use first variable as target (or a composite if available)
    target_var = variables[0]
    feature_vars = variables[1:]
    
    if len(feature_vars) < 2:
        return {'status': 'error', 'message': 'Need at least 3 variables for importance ranking'}
    
    # Prepare data
    X = df[feature_vars].fillna(df[feature_vars].median())
    y = df[target_var].fillna(df[target_var].median())
    
    # Fit Random Forest
    rf = RandomForestRegressor(n_estimators=200, random_state=42)
    rf.fit(X, y)
    
    # Get importance rankings
    importance_df = pd.DataFrame({
        'variable': feature_vars,
        'importance': rf.feature_importances_,
        'importance_percent': rf.feature_importances_ * 100
    }).sort_values('importance', ascending=False)
    
    # Create visualization
    viz_result = _create_importance_visualization(importance_df, session_id)
    
    return {
        'status': 'success',
        'message': f'Variable importance ranking completed using Random Forest',
        'target_variable': target_var,
        'importance_rankings': importance_df.to_dict('records'),
        'model_score': rf.score(X, y),
        'visualization': viz_result
    }


# ========================================
# 3. ADVANCED VISUALIZATIONS
# ========================================

def create_multi_panel_dashboard(session_id: str, variables: List[str] = None) -> Dict[str, Any]:
    """
    Create comprehensive multi-panel dashboard.
    
    HIGH PRIORITY ENHANCEMENT: Provides overview analysis in single view.
    """
    try:
        from app.tools.data_tools import _get_unified_dataset
        df = _get_unified_dataset(session_id)
        
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        # Auto-select variables if not provided
        if not variables:
            var_selection = intelligent_variable_selection(session_id, max_features=4)
            if var_selection['status'] != 'success':
                return var_selection
            variables = var_selection['selected_variables']
        
        # Create subplot dashboard
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                f'Distribution of {variables[0]}',
                f'{variables[0]} vs {variables[1]}',
                f'Correlation Heatmap',
                f'Box Plot Comparison'
            ],
            specs=[[{"type": "histogram"}, {"type": "scatter"}],
                   [{"type": "heatmap"}, {"type": "box"}]]
        )
        
        # Panel 1: Histogram
        fig.add_trace(
            go.Histogram(x=df[variables[0]], name=variables[0], showlegend=False),
            row=1, col=1
        )
        
        # Panel 2: Scatter plot
        fig.add_trace(
            go.Scatter(x=df[variables[0]], y=df[variables[1]], mode='markers',
                      name=f'{variables[0]} vs {variables[1]}', showlegend=False),
            row=1, col=2
        )
        
        # Panel 3: Correlation heatmap
        corr_data = df[variables].corr()
        fig.add_trace(
            go.Heatmap(z=corr_data.values, x=corr_data.columns, y=corr_data.index,
                      colorscale='RdBu', zmid=0, showscale=False),
            row=2, col=1
        )
        
        # Panel 4: Box plot
        fig.add_trace(
            go.Box(y=df[variables[0]], name=variables[0], showlegend=False),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(height=800, title_text="Malaria Risk Analysis Dashboard")
        
        # Save and return
        paths = _save_plotly_chart(fig, session_id, 'dashboard_multi_panel')
        
        return {
            'status': 'success',
            'message': 'Multi-panel dashboard created successfully',
            'chart_type': 'multi_panel_dashboard',
            'variables_analyzed': variables,
            'panels': ['histogram', 'scatter_plot', 'correlation_heatmap', 'box_plot'],
            **paths
        }
        
    except Exception as e:
        logger.error(f"Error creating dashboard: {e}")
        return {'status': 'error', 'message': f'Dashboard creation failed: {str(e)}'}


def violin_plot(session_id: str, variable: str, group_by: str = None) -> Dict[str, Any]:
    """
    Create violin plot for better distribution visualization.
    
    HIGH PRIORITY ENHANCEMENT: Better than box plots for distribution analysis.
    """
    try:
        from app.tools.data_tools import _get_unified_dataset
        df = _get_unified_dataset(session_id)
        
        if df is None:
            return {'status': 'error', 'message': 'No dataset available'}
        
        if variable not in df.columns:
            return {'status': 'error', 'message': f'Variable {variable} not found'}
        
        # Create violin plot
        if group_by and group_by in df.columns:
            fig = go.Figure()
            
            # Add violin for each group
            for group in df[group_by].unique():
                if pd.notna(group):
                    group_data = df[df[group_by] == group][variable].dropna()
                    fig.add_trace(go.Violin(
                        y=group_data,
                        name=str(group),
                        box_visible=True,
                        meanline_visible=True
                    ))
            
            title = f'Distribution of {variable} by {group_by}'
        else:
            fig = go.Figure()
            fig.add_trace(go.Violin(
                y=df[variable].dropna(),
                name=variable,
                box_visible=True,
                meanline_visible=True
            ))
            title = f'Distribution of {variable}'
        
        fig.update_layout(
            title=title,
            yaxis_title=variable,
            height=500,
            template='plotly_white'
        )
        
        paths = _save_plotly_chart(fig, session_id, f'violin_{variable}')
        
        return {
            'status': 'success',
            'message': f'Violin plot created for {variable}',
            'chart_type': 'violin_plot',
            'variable': variable,
            'group_by': group_by,
            **paths
        }
        
    except Exception as e:
        logger.error(f"Error creating violin plot: {e}")
        return {'status': 'error', 'message': f'Violin plot failed: {str(e)}'}


# Helper functions (would need to be integrated into existing codebase)
def _create_cluster_visualization(df: pd.DataFrame, x_var: str, y_var: str, session_id: str):
    """Create cluster visualization."""
    fig = px.scatter(df, x=x_var, y=y_var, color='cluster_name',
                    title='Ward Clustering Analysis')
    return _save_plotly_chart(fig, session_id, 'cluster_analysis')

def _create_importance_visualization(importance_df: pd.DataFrame, session_id: str):
    """Create variable importance visualization."""
    fig = px.bar(importance_df, x='importance_percent', y='variable',
                orientation='h', title='Variable Importance Rankings')
    return _save_plotly_chart(fig, session_id, 'variable_importance')

def _save_plotly_chart(fig, session_id: str, chart_name: str):
    """Save Plotly figure - placeholder function."""
    # This would use the existing _save_plotly_chart function from visual_tools.py
    return {'web_path': f'/charts/{chart_name}.html', 'file_path': f'instance/{chart_name}.html'}


# ========================================
# INTEGRATION INSTRUCTIONS
# ========================================

"""
TO INTEGRATE THESE ENHANCEMENTS INTO CHATMRPT:

1. Add these functions to appropriate tool files:
   - intelligent_variable_selection() → statistical_tools.py
   - machine_learning_analysis() → statistical_tools.py  
   - create_multi_panel_dashboard() → visual_tools.py
   - violin_plot() → visual_tools.py

2. Register new tools in app/tools/__init__.py:
   'intelligent_variable_selection': statistical_tools.intelligent_variable_selection,
   'machine_learning_analysis': statistical_tools.machine_learning_analysis,
   'create_multi_panel_dashboard': visual_tools.create_multi_panel_dashboard,
   'violin_plot': visual_tools.violin_plot,

3. Update request interpreter to recognize new capabilities:
   - "select optimal variables" → intelligent_variable_selection
   - "cluster analysis" → machine_learning_analysis with clustering
   - "create dashboard" → create_multi_panel_dashboard
   - "violin plot" → violin_plot

4. Add required dependencies to requirements.txt:
   - scikit-learn>=1.0.0 (for ML capabilities)
   - plotly>=5.0.0 (already included)

These enhancements provide immediate value while maintaining ChatMRPT's user-friendly approach.
""" 