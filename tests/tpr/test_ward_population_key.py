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


def _shapefile():
    return pd.DataFrame({
        "WardName_norm": ["yelwa", "yelwa", "ribadu", "gombi"],
        "LGAName": ["Mubi North", "Yola North", "Fufore", "Gombi"],
    })


def test_repeated_ward_names_match_by_lga():
    from app.tpr.utils import assign_ward_match_keys

    data = pd.DataFrame({
        "WardName_norm": ["yelwa", "yelwa", "ribadu", "gombi"],
        "LGA": ["Yola North", "ad Mubi North Local Government Area", "Fufore", "Gombi"],
    })
    data_keys, shp_keys = assign_ward_match_keys(data, _shapefile())
    assert list(data_keys) == ["yelwa|yola north", "yelwa|mubi north", "ribadu", "gombi"]
    assert list(shp_keys) == ["yelwa|mubi north", "yelwa|yola north", "ribadu", "gombi"]
    joined = _shapefile().assign(k=shp_keys.values).merge(data.assign(k=data_keys.values), on="k")
    assert len(joined) == 4  # one row per ward, no cross-matching


def test_repeated_name_in_data_only_uses_lga():
    from app.tpr.utils import assign_ward_match_keys

    data = pd.DataFrame({"WardName_norm": ["ribadu", "ribadu"], "LGA": ["Fufore", "Mayo-Belwa"]})
    data_keys, shp_keys = assign_ward_match_keys(data, _shapefile())
    assert list(data_keys) == ["ribadu|fufore", "ribadu|unmatched:mayo belwa"]
    assert "ribadu|fufore" in list(shp_keys)


def test_close_lga_spelling_still_matches():
    from app.tpr.utils import assign_ward_match_keys

    data = pd.DataFrame({"WardName_norm": ["yelwa"], "LGA": ["Yola-Nort"]})
    data_keys, _ = assign_ward_match_keys(data, _shapefile())
    assert list(data_keys) == ["yelwa|yola north"]


def test_unique_names_unchanged():
    from app.tpr.utils import assign_ward_match_keys

    shp = pd.DataFrame({"WardName_norm": ["a", "b"], "LGAName": ["X", "Y"]})
    data = pd.DataFrame({"WardName_norm": ["a", "b"], "LGA": ["typo", "Y"]})
    data_keys, shp_keys = assign_ward_match_keys(data, shp)
    assert list(data_keys) == ["a", "b"] and list(shp_keys) == ["a", "b"]


def test_precompute_db_keeps_same_named_wards_in_different_lgas(tmp_path, monkeypatch):
    import sqlite3

    import app.tpr.precompute as pc

    db = tmp_path / "pre.db"
    monkeypatch.setattr(pc, "get_precompute_db_path", lambda sid: str(db))
    pc.init_precompute_db("s")
    con = sqlite3.connect(db)
    rows = pd.DataFrame({
        "WardName": ["yelwa", "yelwa"], "LGA": ["Yola North", "Mubi North"],
        "Burden": [77.8, 53.2], "Population": [1273, 7951], "Total_Positive": [99, 423],
    })
    pc._store_tpr_df(con.cursor(), rows, "primary", "u5")
    con.commit()
    assert con.execute("select count(*) from tpr_results").fetchone()[0] == 2


def test_old_precompute_db_is_migrated_without_losing_rows(tmp_path, monkeypatch):
    import sqlite3

    import app.tpr.precompute as pc

    db = tmp_path / "old.db"
    con = sqlite3.connect(db)
    con.execute("""CREATE TABLE tpr_results (id INTEGER PRIMARY KEY AUTOINCREMENT,
        facility_level TEXT NOT NULL, age_group TEXT NOT NULL, ward_name TEXT NOT NULL, lga TEXT,
        burden REAL, population INTEGER, total_positive INTEGER,
        UNIQUE(facility_level, age_group, ward_name))""")
    con.execute("INSERT INTO tpr_results (facility_level, age_group, ward_name, lga, burden) "
                "VALUES ('all','u5','gombi','Gombi',12.0), ('all','u5','x',NULL,1.0)")
    con.commit(); con.close()

    monkeypatch.setattr(pc, "get_precompute_db_path", lambda sid: str(db))
    pc.init_precompute_db("s")
    con = sqlite3.connect(db)
    assert con.execute("select count(*) from tpr_results").fetchone()[0] == 2
    sql = con.execute("select sql from sqlite_master where name='tpr_results'").fetchone()[0]
    assert "ward_name, lga)" in sql
    assert con.execute("select count(*) from sqlite_master where name='tpr_results_old'").fetchone()[0] == 0
