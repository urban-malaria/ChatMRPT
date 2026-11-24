"""Centre and zoom that fit a set of ward shapes in a Plotly map.

Plotly's tile maps take a centre and a zoom level rather than bounds. These are computed from the
shapes' extent with Web Mercator geometry, so a map opens showing the whole state and little else.
"""
from __future__ import annotations

import math
from typing import Dict, Sequence, Union

TILE_SIZE = 512  # Mapbox GL zoom levels are defined for 512-pixel tiles


def _mercator_y(lat: float) -> float:
    lat = max(min(lat, 85.0), -85.0)
    return math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))


def fit_view(shapes_or_bounds: Union[Sequence[float], object], width: int = 800, height: int = 550,
             padding: float = 0.9) -> Dict[str, object]:
    """{'center': {'lat', 'lon'}, 'zoom'} so that the shapes fill about `padding` of a width x height map.

    `shapes_or_bounds` is a GeoDataFrame/GeoSeries or (min_lon, min_lat, max_lon, max_lat). The default
    size is a little smaller than the chat's map frame so the whole area stays visible.
    """
    bounds = getattr(shapes_or_bounds, 'total_bounds', shapes_or_bounds)
    min_lon, min_lat, max_lon, max_lat = (float(v) for v in bounds)
    lon_span = max(max_lon - min_lon, 1e-4)
    y_span = max(_mercator_y(max_lat) - _mercator_y(min_lat), 1e-4)

    zoom_x = math.log2(width * padding * 360 / (TILE_SIZE * lon_span))
    zoom_y = math.log2(height * padding * 2 * math.pi / (TILE_SIZE * y_span))
    zoom = max(3.0, min(14.0, min(zoom_x, zoom_y)))

    center_y = (_mercator_y(max_lat) + _mercator_y(min_lat)) / 2
    center_lat = math.degrees(2 * math.atan(math.exp(center_y)) - math.pi / 2)
    return {'center': {'lat': center_lat, 'lon': (min_lon + max_lon) / 2}, 'zoom': round(zoom, 2)}
