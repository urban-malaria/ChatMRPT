"""The composite score and PCA risk maps side by side, on one colour scale."""
import logging
from datetime import datetime
from typing import Any, Dict

import geopandas as gpd
import plotly.graph_objects as go

from app.utils.map_view import fit_view
from .composite import create_agent_vulnerability_map
from .geo_utils import RISK_COLORSCALE, save_agent_visualization
from .pca import create_agent_pca_vulnerability_map

logger = logging.getLogger(__name__)

PANEL_GAP = 0.02
FIGURE_HEIGHT = 520


def create_risk_method_comparison_map(unified_dataset: gpd.GeoDataFrame,
                                      session_id: str = 'default') -> Dict[str, Any]:
    """One figure: composite score ranking on the left, PCA ranking on the right, one shared legend."""
    composite = create_agent_vulnerability_map(unified_dataset, session_id, return_figure=True)
    pca = create_agent_pca_vulnerability_map(unified_dataset, session_id, return_figure=True)
    if composite.get('status') != 'success' or pca.get('status') != 'success':
        return {'status': 'error',
                'message': composite.get('message') or pca.get('message') or 'Could not draw the risk maps'}

    panels = [('mapbox', composite['fig'], 'Composite score'), ('mapbox2', pca['fig'], 'PCA')]
    choropleths = [t for _, fig, _ in panels for t in fig.data if t.type == 'choroplethmapbox']
    zmax = max(t.zmax or 1 for t in choropleths)

    fig = go.Figure()
    for subplot, source, _ in panels:
        for trace in source.data:
            trace.subplot = subplot
            if trace.type == 'choroplethmapbox':
                # Same scale and range on both panels, so a colour means the same rank on each.
                trace.update(colorscale=RISK_COLORSCALE, zmin=1, zmax=zmax, marker_opacity=0.8,
                             showscale=subplot == 'mapbox')
            fig.add_trace(trace)

    legend = choropleths[0].colorbar
    fig.data[0].colorbar = dict(title=dict(text='Ward rank', side='top'), tickmode='array',
                                tickvals=legend.tickvals, ticktext=legend.ticktext,
                                orientation='h', x=0.5, xanchor='center', y=-0.02, yanchor='top',
                                len=0.6, thickness=12)

    # Fit the state into each half of the figure.
    view = fit_view(unified_dataset.to_crs(epsg=4326).total_bounds, width=380, height=FIGURE_HEIGHT - 140)
    half = (1 - PANEL_GAP) / 2
    domains = {'mapbox': [0, half], 'mapbox2': [1 - half, 1]}
    titles = []
    for subplot, _, title in panels:
        fig.update_layout({subplot: dict(style='open-street-map', center=view['center'], zoom=view['zoom'],
                                         domain=dict(x=domains[subplot], y=[0, 0.92]))})
        titles.append(dict(text=f'<b>{title}</b>', x=sum(domains[subplot]) / 2, y=0.98, xref='paper',
                           yref='paper', showarrow=False, font=dict(size=14)))

    fig.update_layout(title=dict(text='Ward risk ranking: both methods', x=0.5, xanchor='center',
                                 font=dict(size=18)),
                      annotations=titles, height=FIGURE_HEIGHT, margin=dict(l=10, r=10, t=60, b=70),
                      showlegend=False)

    filename = f"vulnerability_map_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    saved = save_agent_visualization(fig, filename, session_id, 'vulnerability_maps')
    if saved.get('status') != 'success':
        return saved
    return {'status': 'success', 'file_path': saved['file_path'], 'web_path': saved['web_path'],
            'visualization_type': 'vulnerability_map_comparison'}
