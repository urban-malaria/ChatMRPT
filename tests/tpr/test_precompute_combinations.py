import sqlite3

import pandas as pd

from app.tpr import precompute


def _fake_tpr_df(facility_level, age_group):
    return pd.DataFrame({
        "WardName": [f"{facility_level}-{age_group}-ward"],
        "LGA": ["LGA"],
        "Burden": [12.5],
        "Population": [1000],
        "Total_Positive": [13],
    })


def test_precompute_all_combinations_includes_original_selection(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_id = "combo-session"

    def fake_calculate_ward_tpr(df, age_group, test_method, facility_level, schema, state_gdf, pop_cache):
        return _fake_tpr_df(facility_level, age_group)

    monkeypatch.setattr("app.tpr.utils.calculate_ward_tpr", fake_calculate_ward_tpr)

    result = precompute.precompute_all_tpr_combinations(
        session_id=session_id,
        data=pd.DataFrame({"x": [1]}),
        state="Kwara",
        exclude_combination={"facility_level": "primary", "age_group": "u5"},
        state_gdf=pd.DataFrame({"WardName_norm": ["alpha"]}),
        pop_cache={},
    )

    assert result["combinations_computed"] == 16
    assert result["combinations_skipped"] == 0
    assert precompute.has_precomputed_combination(session_id, "primary", "u5")

    conn = sqlite3.connect(precompute.get_precompute_db_path(session_id))
    count = conn.execute("select count(*) from computation_status").fetchone()[0]
    conn.close()
    assert count == 16


def test_repair_precomputed_combination_restores_missing_original(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_id = "repair-session"
    session_folder = tmp_path / "instance" / "uploads" / session_id
    session_folder.mkdir(parents=True)
    (session_folder / ".agent_state.json").write_text(
        '{"_session_id":"repair-session","column_schema":{"ward":"Ward","state":"State"},"tpr_selections":{"state":"Kwara","facility_level":"primary","age_group":"u5"}}',
        encoding="utf-8",
    )
    pd.DataFrame({"Ward": ["Alpha"], "State": ["Kwara"]}).to_csv(session_folder / "uploaded_data.csv", index=False)

    db_path = precompute.init_precompute_db(session_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    precompute._store_tpr_df(cursor, _fake_tpr_df("all", "all_ages"), "all", "all_ages")
    conn.commit()
    conn.close()

    def fake_calculate_ward_tpr(df, age_group, test_method, facility_level, schema, state_gdf, pop_cache):
        return _fake_tpr_df(facility_level, age_group)

    monkeypatch.setattr("app.tpr.utils.calculate_ward_tpr", fake_calculate_ward_tpr)

    assert not precompute.has_precomputed_combination(session_id, "primary", "u5")
    repaired = precompute.repair_precomputed_combination(
        session_id,
        "primary",
        "u5",
        state_gdf=pd.DataFrame({"WardName_norm": ["alpha"]}),
        pop_cache={},
    )

    assert repaired["success"] is True
    assert repaired["repaired"] is True
    assert precompute.has_precomputed_combination(session_id, "primary", "u5")

    queried = precompute.query_precomputed_tpr(session_id, "primary", "u5")
    assert queried["success"] is True
    assert queried["summary"]["total_wards"] == 1
