"""Population lookups must work when ward names repeat across LGAs."""

import pandas as pd

from app.tpr.utils import ward_population_key


def _wards():
    return pd.DataFrame({
        "WardName_norm": ["yelwa", "yelwa", "ribadu"],
        "LGACode": ["L1", "L2", "L1"],
    })


def test_key_is_unique_when_names_repeat_across_lgas():
    key = ward_population_key(_wards())
    assert key.is_unique
    assert list(key) == ["yelwa|L1", "yelwa|L2", "ribadu|L1"]


def test_population_maps_to_correct_duplicate_named_ward():
    gdf = _wards()
    cache = pd.Series([100.0, 200.0, 300.0], index=ward_population_key(gdf).values)
    merged = gdf.iloc[[1, 0, 2]].reset_index(drop=True)  # merge reorders rows
    assert list(ward_population_key(merged).map(cache)) == [200.0, 100.0, 300.0]


def test_key_falls_back_to_name_without_lga_column():
    gdf = pd.DataFrame({"WardName_norm": ["a", "b"]})
    assert list(ward_population_key(gdf)) == ["a", "b"]
