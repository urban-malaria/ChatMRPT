import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from app.settlement import SettlementClassificationService


def _write_session(tmp_path):
    upload_root = tmp_path / "uploads"
    export_root = tmp_path / "exports"
    session_id = "settlement-test-session"
    session_folder = upload_root / session_id
    shapefile_dir = session_folder / "shapefile"
    shapefile_dir.mkdir(parents=True)

    pd.DataFrame({
        "WardName": ["Alpha", "Beta"],
        "value": [1, 2],
    }).to_csv(session_folder / "raw_data.csv", index=False)

    gdf = gpd.GeoDataFrame(
        {
            "WardName": ["Alpha", "Beta"],
            "WardCode": ["A001", "B001"],
        },
        geometry=[
            Polygon([(3.0, 8.0), (3.02, 8.0), (3.02, 8.02), (3.0, 8.02)]),
            Polygon([(3.05, 8.0), (3.07, 8.0), (3.07, 8.02), (3.05, 8.02)]),
        ],
        crs="EPSG:4326",
    )
    gdf.to_file(shapefile_dir / "raw.shp")

    return session_id, upload_root, export_root


def test_create_classification_save_annotation_and_export(tmp_path):
    session_id, upload_root, export_root = _write_session(tmp_path)
    service = SettlementClassificationService(
        session_id,
        upload_root=str(upload_root),
        export_root=str(export_root),
    )

    result = service.create_classification(ward_names=["Alpha"], cell_size_m=1000)

    assert result["classification_id"].startswith("settlement-")
    assert result["grid_cell_count"] > 0
    assert result["selected_wards"][0]["ward_code"] == "A001"

    grid = service.load_grid_geojson(result["classification_id"])
    first_grid_id = grid["features"][0]["properties"]["grid_id"]

    saved = service.save_annotation(
        result["classification_id"],
        {
            "grid_id": first_grid_id,
            "label": "Formal",
            "notes": "planned roads and regular blocks",
        },
    )

    assert saved["success"] is True
    assert saved["annotation"]["label"] == "Formal"

    annotations = service.load_annotations(result["classification_id"])
    assert annotations["annotations"][first_grid_id]["notes"] == "planned roads and regular blocks"

    export = service.export_classification(result["classification_id"])
    assert export["success"] is True
    assert (export_root / session_id / f"settlement_export_{result['classification_id']}" / "settlement_annotations.csv").exists()
    assert (export_root / session_id / f"settlement_export_{result['classification_id']}" / "settlement_classified_grid.geojson").exists()


def test_top_n_classification_uses_composite_rankings(tmp_path):
    session_id, upload_root, export_root = _write_session(tmp_path)
    session_folder = upload_root / session_id
    pd.DataFrame({
        "WardCode": ["B001", "A001"],
        "overall_rank": [1, 2],
    }).to_csv(session_folder / "analysis_vulnerability_rankings.csv", index=False)

    service = SettlementClassificationService(
        session_id,
        upload_root=str(upload_root),
        export_root=str(export_root),
    )

    result = service.create_classification(top_n=1, method="composite", cell_size_m=1000)

    assert result["selected_wards"][0]["ward_code"] == "B001"


def test_generic_classification_defaults_to_first_ward_without_rankings(tmp_path):
    session_id, upload_root, export_root = _write_session(tmp_path)
    service = SettlementClassificationService(
        session_id,
        upload_root=str(upload_root),
        export_root=str(export_root),
    )

    result = service.create_classification(cell_size_m=1000)

    assert result["selected_wards"][0]["ward_code"] == "A001"


def test_rejects_invalid_annotation_label(tmp_path):
    session_id, upload_root, export_root = _write_session(tmp_path)
    service = SettlementClassificationService(
        session_id,
        upload_root=str(upload_root),
        export_root=str(export_root),
    )
    result = service.create_classification(ward_names=["Alpha"], cell_size_m=1000)
    grid = service.load_grid_geojson(result["classification_id"])
    first_grid_id = grid["features"][0]["properties"]["grid_id"]

    try:
        service.save_annotation(
            result["classification_id"],
            {"grid_id": first_grid_id, "label": "Unsafe Label", "notes": ""},
        )
    except ValueError as exc:
        assert "Invalid settlement label" in str(exc)
    else:
        raise AssertionError("Invalid label was accepted")
