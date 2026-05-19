import importlib
import os


def test_default_raster_dir_points_to_geospatial_rasters(monkeypatch):
    monkeypatch.delenv("CHATMRPT_RASTER_DIR", raising=False)

    from app.config import data_paths

    importlib.reload(data_paths)

    assert data_paths.RASTER_DIR == os.path.join(
        data_paths.PROJECT_ROOT,
        "data",
        "geospatial",
        "rasters",
    )


def test_raster_dir_env_override_still_takes_precedence(monkeypatch):
    monkeypatch.setenv("CHATMRPT_RASTER_DIR", "/mnt/efs/chatmrpt/rasters")

    from app.config import data_paths

    importlib.reload(data_paths)

    assert data_paths.RASTER_DIR == "/mnt/efs/chatmrpt/rasters"
