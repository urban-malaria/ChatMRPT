import math

from app.utils.map_view import fit_view

KWARA = (2.73, 7.75, 6.75, 10.15)   # roughly 4 degrees wide


def test_state_extent_gets_a_state_level_zoom():
    view = fit_view(KWARA)
    assert 6.5 <= view['zoom'] <= 7.5          # the old formula gave 4.6, about all of West Africa
    assert math.isclose(view['center']['lon'], 4.74, abs_tol=0.01)
    assert 8.9 < view['center']['lat'] < 9.0


def test_smaller_area_zooms_in_further():
    assert fit_view((3.8, 7.3, 4.0, 7.5))['zoom'] > fit_view(KWARA)['zoom'] + 3
