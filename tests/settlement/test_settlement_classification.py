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
            "LGA": ["One", "Two"],
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


def test_selector_map_and_boundaries_include_filter_properties(tmp_path):
    session_id, upload_root, export_root = _write_session(tmp_path)
    service = SettlementClassificationService(
        session_id,
        upload_root=str(upload_root),
        export_root=str(export_root),
    )

    selector = service.create_selector_map(cell_size_m=1000)
    boundaries = service.load_boundaries_geojson()

    assert selector["selector"] is True
    assert selector["ward_count"] == 2
    assert selector["lga_count"] == 2
    selector_html = (upload_root / session_id / "settlement" / "selector" / "settlement_selector.html").read_text(encoding="utf-8")
    assert "classifications/estimate" in selector_html
    assert "classificationList" in selector_html
    assert "resumeClassification" in selector_html
    assert "layerRegistry" in selector_html
    assert "clearFocus" in selector_html
    assert "Fit State" in selector_html
    assert "Fit Grid" in selector_html
    assert "searchInput" in selector_html
    assert "visibleFeatureList" in selector_html
    assert "renderSearchResults" in selector_html
    assert "updateVisibleFeatureList" in selector_html
    assert "selectWardFocus" in selector_html
    assert boundaries["features"][0]["properties"]["ward_id"]
    assert {feature["properties"]["lga"] for feature in boundaries["features"]} == {"One", "Two"}


def test_estimate_list_archive_and_duplicate_classification(tmp_path):
    session_id, upload_root, export_root = _write_session(tmp_path)
    service = SettlementClassificationService(
        session_id,
        upload_root=str(upload_root),
        export_root=str(export_root),
    )

    estimate = service.estimate_classification(ward_ids=["A001"], cell_size_m=1000)
    assert estimate["success"] is True
    assert estimate["allowed"] is True
    assert estimate["selected_ward_count"] == 1
    assert estimate["estimated_cell_count"] > 0

    created = service.create_classification(ward_ids=["A001"], cell_size_m=1000)
    grid = service.load_grid_geojson(created["classification_id"])
    first_grid_id = grid["features"][0]["properties"]["grid_id"]
    service.save_annotation(
        created["classification_id"],
        {"grid_id": first_grid_id, "label": "Formal", "notes": "sample"},
    )

    items = service.list_classifications()
    assert [item["classification_id"] for item in items] == [created["classification_id"]]
    assert items[0]["classified_count"] == 1
    assert items[0]["progress_percent"] > 0

    duplicated = service.duplicate_classification(created["classification_id"])
    assert duplicated["classification_id"] != created["classification_id"]
    assert duplicated["selected_wards"][0]["ward_id"] == "A001"

    archived = service.archive_classification(created["classification_id"])
    assert archived["archived"] is True
    active_ids = {item["classification_id"] for item in service.list_classifications()}
    assert created["classification_id"] not in active_ids
    archived_ids = {item["classification_id"] for item in service.list_classifications(include_archived=True)}
    assert created["classification_id"] in archived_ids


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
