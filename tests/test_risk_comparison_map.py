"""The composite and PCA maps are drawn side by side on one colour scale."""
import json
import re

import geopandas as gpd
from shapely.geometry import box

from app.visualization.geo_utils import RISK_COLORSCALE
from app.visualization.risk_comparison import create_risk_method_comparison_map


def _wards():
    return gpd.GeoDataFrame({
        'WardName': ['Alpha', 'Beta', 'Gamma', 'Delta'],
        'LGA': ['L1', 'L1', 'L2', 'L2'],
        'composite_score': [0.9, 0.6, 0.4, 0.1],
        'composite_rank': [1, 2, 3, 4],
        'composite_category': ['Very High', 'High', 'Medium', 'Low'],
        'pca_score': [0.8, 0.9, 0.2, 0.1],
        'pca_rank': [2, 1, 3, 4],
        'pca_category': ['High', 'Very High', 'Medium', 'Low'],
    }, geometry=[box(4 + i * 0.1, 8, 4.1 + i * 0.1, 8.1) for i in range(4)], crs='EPSG:4326')


def test_both_panels_share_one_scale(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = create_risk_method_comparison_map(_wards(), 'test-session')
    assert result['status'] == 'success'

    html = (tmp_path / result['file_path']).read_text()
    traces = json.loads(re.search(r'Plotly\.newPlot\(\s*"[^"]+",\s*(\[.*?\]),\s*\{', html, re.S).group(1))
    maps = [t for t in traces if t['type'] == 'choroplethmapbox']
    assert [t['subplot'] for t in maps] == ['mapbox', 'mapbox2']
    assert all((t['zmin'], t['zmax']) == (1, 4) for t in maps)
    assert sum(t.get('showscale', True) for t in maps) == 1
    assert maps[0]['colorscale'] == maps[1]['colorscale']
    assert RISK_COLORSCALE == 'YlOrRd'
