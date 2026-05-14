"""Manual grid-based settlement classification services.

This module implements the Shiny-style settlement labeling workflow used by
ChatMRPT. It intentionally does not use the legacy building-footprint settlement
loader; the workflow here is manual grid classification over uploaded ward
boundaries.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import geopandas as gpd
import pandas as pd
from pydantic import Field, validator
from shapely.geometry import box

from app.services.data_handler import DataHandler
from app.utils.tool_base import BaseTool, ToolCategory, ToolExecutionResult

logger = logging.getLogger(__name__)


DEFAULT_LABELS = ["Formal", "Informal", "Slum", "No Buildings/Avoid Area"]
WARD_NAME_CANDIDATES = [
    "WardName",
    "ward_name",
    "Ward_Name",
    "WARD_NAME",
    "ward",
    "Ward",
    "ADM3_NAME",
    "admin3Name",
    "NAME_3",
]
WARD_CODE_CANDIDATES = [
    "WardCode",
    "ward_code",
    "WARD_CODE",
    "ADM3_CODE",
    "admin3Pcode",
    "pcode",
    "Ward_ID",
    "ward_id",
]
LGA_CANDIDATES = ["LGA", "Lga", "lga", "LGAName", "lga_name", "ADM2_NAME", "NAME_2"]
STATE_CANDIDATES = ["State", "state", "StateName", "state_name", "ADM1_NAME", "NAME_1"]
RANK_CANDIDATES = [
    "overall_rank",
    "Overall_Rank",
    "Rank",
    "rank",
    "composite_rank",
    "Composite_Rank",
    "risk_rank",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_slug(value: str, fallback: str = "item") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip()).strip("-")
    return cleaned[:80] or fallback


def _normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _first_existing(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    column_set = set(columns)
    for candidate in candidates:
        if candidate in column_set:
            return candidate
    lowered = {str(col).lower(): col for col in columns}
    for candidate in candidates:
        found = lowered.get(candidate.lower())
        if found:
            return found
    return None


def _json_responseable_path(path: Path) -> str:
    return str(path).replace("\\", "/")


class SettlementClassificationService:
    """Create, persist, and export manual settlement classifications."""

    def __init__(self, session_id: str, upload_root: str = "instance/uploads", export_root: str = "instance/exports"):
        self.session_id = session_id
        self.upload_root = Path(upload_root).resolve()
        self.export_root = Path(export_root).resolve()
        self.session_folder = (self.upload_root / session_id).resolve()
        self.settlement_root = self.session_folder / "settlement"

    @property
    def handler(self) -> DataHandler:
        return DataHandler(str(self.session_folder))

    def status(self) -> Dict[str, Any]:
        dh = self.handler
        has_data = dh.csv_data is not None
        has_shapefile = dh.shapefile_data is not None and not dh.shapefile_data.empty
        rankings = self._load_rankings("composite")
        pca_rankings = self._load_rankings("pca")
        return {
            "success": True,
            "session_id": self.session_id,
            "has_data": has_data,
            "has_shapefile": has_shapefile,
            "has_composite_rankings": rankings is not None and not rankings.empty,
            "has_pca_rankings": pca_rankings is not None and not pca_rankings.empty,
        }

    def list_wards(self, include_rankings: bool = True, method: str = "composite") -> List[Dict[str, Any]]:
        gdf = self._load_shapefile()
        prepared, meta = self._prepare_ward_gdf(gdf)
        records: List[Dict[str, Any]] = []
        ranking_lookup = self._build_ranking_lookup(method) if include_rankings else {}

        for _, row in prepared.drop(columns="geometry", errors="ignore").iterrows():
            ward_id = str(row["_settlement_ward_id"])
            rank_info = ranking_lookup.get(ward_id) or ranking_lookup.get(_normalize(row.get(meta["name_col"])))
            record = {
                "ward_id": ward_id,
                "display_name": row["_settlement_display_name"],
                "ward_name": row.get(meta["name_col"]),
                "ward_code": row.get(meta["code_col"]) if meta.get("code_col") else None,
                "lga": row.get(meta["lga_col"]) if meta.get("lga_col") else None,
                "state": row.get(meta["state_col"]) if meta.get("state_col") else None,
                "duplicated_name": bool(row["_settlement_duplicated_name"]),
            }
            if rank_info:
                record.update(rank_info)
            records.append(record)

        return sorted(records, key=lambda item: (item.get("rank") is None, item.get("rank") or 999999, item["display_name"]))

    def load_boundaries_geojson(self, method: str = "composite") -> Dict[str, Any]:
        """Return prepared ward boundaries for the overview selector map."""
        gdf = self._load_shapefile()
        prepared, meta = self._prepare_ward_gdf(gdf)
        ranking_lookup = self._build_ranking_lookup(method)

        records = prepared.copy()
        records = records.to_crs(epsg=4326)
        try:
            records["geometry"] = records.geometry.simplify(0.00005, preserve_topology=True)
        except Exception:
            logger.debug("Boundary simplification failed", exc_info=True)

        records["ward_id"] = records["_settlement_ward_id"]
        records["display_name"] = records["_settlement_display_name"]
        records["ward_name"] = records[meta["name_col"]]
        records["ward_code"] = records[meta["code_col"]] if meta.get("code_col") else None
        records["lga"] = records[meta["lga_col"]] if meta.get("lga_col") else None
        records["state"] = records[meta["state_col"]] if meta.get("state_col") else None
        records["rank"] = None

        for idx, row in records.iterrows():
            rank_info = ranking_lookup.get(str(row["ward_id"])) or ranking_lookup.get(_normalize(row.get("ward_name")))
            if rank_info:
                records.at[idx, "rank"] = rank_info.get("rank")

        keep_cols = [
            "ward_id",
            "display_name",
            "ward_name",
            "ward_code",
            "lga",
            "state",
            "rank",
            "geometry",
        ]
        return json.loads(records[keep_cols].to_json())

    def create_selector_map(
        self,
        method: str = "composite",
        cell_size_m: int = 500,
        include_no_buildings: bool = True,
    ) -> Dict[str, Any]:
        """Create the full-state overview selector before grid generation."""
        gdf = self._load_shapefile()
        prepared, meta = self._prepare_ward_gdf(gdf)
        selector_dir = self.settlement_root / "selector"
        selector_dir.mkdir(parents=True, exist_ok=True)
        html_path = selector_dir / "settlement_selector.html"

        wards = self.list_wards(include_rankings=True, method=method)
        lgas = sorted({
            str(item.get("lga")).strip()
            for item in wards
            if item.get("lga") not in (None, "")
        })
        states = sorted({
            str(item.get("state")).strip()
            for item in wards
            if item.get("state") not in (None, "")
        })
        metadata = {
            "session_id": self.session_id,
            "method": method,
            "cell_size_m": cell_size_m,
            "include_no_buildings": include_no_buildings,
            "ward_count": int(len(prepared)),
            "lga_count": len(lgas),
            "state_names": states,
            "name_column": meta["name_col"],
            "lga_column": meta.get("lga_col"),
        }
        html_path.write_text(self._render_selector_html(metadata, wards, lgas), encoding="utf-8")
        return {
            "selector": True,
            "file_path": _json_responseable_path(html_path),
            "web_path": f"/serve_viz_file/{self.session_id}/settlement/selector/settlement_selector.html",
            "ward_count": int(len(prepared)),
            "lga_count": len(lgas),
            "message": "Opened settlement classification selector.",
            "download_links": [],
        }

    def create_classification(
        self,
        ward_names: Optional[Sequence[str]] = None,
        ward_ids: Optional[Sequence[str]] = None,
        top_n: Optional[int] = None,
        method: str = "composite",
        cell_size_m: int = 500,
        include_no_buildings: bool = True,
        max_cells: int = 2500,
    ) -> Dict[str, Any]:
        if cell_size_m < 100:
            raise ValueError("Cell size is too small. Use at least 100 meters.")
        if cell_size_m > 5000:
            raise ValueError("Cell size is too large. Use 5000 meters or less.")

        gdf = self._load_shapefile()
        prepared, meta = self._prepare_ward_gdf(gdf)
        selected, selection_message = self._select_wards(prepared, meta, ward_names, ward_ids, top_n, method)
        if selected.empty:
            raise ValueError("No wards matched the classification request.")

        grid = self._build_grid(selected, cell_size_m=cell_size_m, max_cells=max_cells)
        if grid.empty:
            raise ValueError("No grid cells were generated for the selected wards.")

        classification_id = self._new_classification_id(selected, cell_size_m)
        class_dir = self._classification_dir(classification_id)
        class_dir.mkdir(parents=True, exist_ok=True)
        grid["classification_id"] = classification_id

        labels = DEFAULT_LABELS if include_no_buildings else ["Formal", "Informal", "Slum"]
        grid_path = class_dir / "grid.geojson"
        annotations_path = class_dir / "annotations.json"
        html_path = class_dir / "classifier.html"
        metadata_path = class_dir / "metadata.json"

        grid.to_file(grid_path, driver="GeoJSON")
        self._write_json_atomic(annotations_path, {"annotations": {}, "updated_at": _utc_now()})

        wards_summary = [
            {
                "ward_id": str(row["_settlement_ward_id"]),
                "display_name": row["_settlement_display_name"],
                "ward_name": row.get(meta["name_col"]),
                "ward_code": row.get(meta["code_col"]) if meta.get("code_col") else None,
            }
            for _, row in selected.drop(columns="geometry", errors="ignore").iterrows()
        ]

        metadata = {
            "classification_id": classification_id,
            "session_id": self.session_id,
            "created_at": _utc_now(),
            "updated_at": _utc_now(),
            "cell_size_m": cell_size_m,
            "labels": labels,
            "method": method,
            "selection_message": selection_message,
            "wards": wards_summary,
            "grid_cell_count": int(len(grid)),
            "grid_path": _json_responseable_path(grid_path),
            "annotations_path": _json_responseable_path(annotations_path),
        }
        self._write_json_atomic(metadata_path, metadata)
        html_path.write_text(self._render_classifier_html(metadata), encoding="utf-8")

        self.export_classification(classification_id, include_static_map=False)

        web_path = f"/serve_viz_file/{self.session_id}/settlement/{classification_id}/classifier.html"
        return {
            "classification_id": classification_id,
            "file_path": _json_responseable_path(html_path),
            "web_path": web_path,
            "grid_cell_count": int(len(grid)),
            "selected_wards": wards_summary,
            "message": selection_message,
            "download_links": self._download_links(classification_id),
        }

    def get_classification(self, classification_id: str) -> Dict[str, Any]:
        return self._load_metadata(classification_id)

    def load_grid_geojson(self, classification_id: str) -> Dict[str, Any]:
        grid_path = self._classification_dir(classification_id) / "grid.geojson"
        self._ensure_inside_session(grid_path)
        if not grid_path.exists():
            raise FileNotFoundError("Grid file not found.")
        return json.loads(grid_path.read_text(encoding="utf-8"))

    def load_annotations(self, classification_id: str) -> Dict[str, Any]:
        path = self._classification_dir(classification_id) / "annotations.json"
        self._ensure_inside_session(path)
        if not path.exists():
            return {"annotations": {}, "updated_at": None}
        return json.loads(path.read_text(encoding="utf-8"))

    def save_annotation(self, classification_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        metadata = self._load_metadata(classification_id)
        grid_id = str(payload.get("grid_id") or "").strip()
        if not grid_id:
            raise ValueError("grid_id is required.")

        label = str(payload.get("label") or "").strip()
        if label not in metadata.get("labels", DEFAULT_LABELS):
            raise ValueError(f"Invalid settlement label: {label}")

        notes = str(payload.get("notes") or "").strip()
        if len(notes) > 1000:
            raise ValueError("Notes must be 1000 characters or fewer.")

        valid_grid = self._valid_grid_ids(classification_id)
        if grid_id not in valid_grid:
            raise ValueError("Unknown grid_id for this classification.")

        annotations_doc = self.load_annotations(classification_id)
        annotations = annotations_doc.setdefault("annotations", {})
        now = _utc_now()
        grid_props = valid_grid[grid_id]
        annotations[grid_id] = {
            "classification_id": classification_id,
            "grid_id": grid_id,
            "ward_id": grid_props.get("ward_id"),
            "ward_name": grid_props.get("ward_name"),
            "label": label,
            "notes": notes,
            "updated_at": now,
            "updated_by_session": self.session_id,
        }
        annotations_doc["updated_at"] = now

        annotations_path = self._classification_dir(classification_id) / "annotations.json"
        self._write_json_atomic(annotations_path, annotations_doc)
        self._write_annotations_csv(classification_id, annotations_doc)
        self._touch_metadata(classification_id)
        return {"success": True, "annotation": annotations[grid_id], "count": len(annotations)}

    def export_classification(self, classification_id: str, include_static_map: bool = False) -> Dict[str, Any]:
        metadata = self._load_metadata(classification_id)
        export_dir = self._export_dir(classification_id)
        export_dir.mkdir(parents=True, exist_ok=True)

        annotations_doc = self.load_annotations(classification_id)
        self._write_annotations_csv(classification_id, annotations_doc)
        grid = gpd.read_file(self._classification_dir(classification_id) / "grid.geojson")
        annotations = pd.DataFrame(list(annotations_doc.get("annotations", {}).values()))
        if not annotations.empty:
            grid = grid.merge(annotations[["grid_id", "label", "notes", "updated_at"]], on="grid_id", how="left")
        else:
            grid["label"] = None
            grid["notes"] = None
            grid["updated_at"] = None

        csv_path = export_dir / "settlement_annotations.csv"
        geojson_path = export_dir / "settlement_classified_grid.geojson"
        metadata_path = export_dir / "settlement_metadata.json"

        pd.DataFrame(list(annotations_doc.get("annotations", {}).values())).to_csv(csv_path, index=False)
        grid.to_file(geojson_path, driver="GeoJSON")
        self._write_json_atomic(metadata_path, metadata)

        files = [
            {"path": csv_path, "type": "csv", "description": "Settlement annotations CSV"},
            {"path": geojson_path, "type": "geojson", "description": "Classified settlement grid GeoJSON"},
            {"path": metadata_path, "type": "json", "description": "Settlement classification metadata"},
        ]
        return {
            "success": True,
            "classification_id": classification_id,
            "export_dir": _json_responseable_path(export_dir),
            "files": files,
            "download_links": self._download_links(classification_id),
        }

    def _load_shapefile(self) -> gpd.GeoDataFrame:
        dh = self.handler
        if dh.shapefile_data is None or dh.shapefile_data.empty:
            raise FileNotFoundError("No shapefile found for this session. Upload or generate ward boundaries first.")
        return dh.shapefile_data.copy()

    def _prepare_ward_gdf(self, gdf: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, Dict[str, Optional[str]]]:
        gdf = gdf.copy()
        if gdf.crs is None:
            logger.warning("Uploaded shapefile has no CRS; assuming EPSG:4326")
            gdf = gdf.set_crs(epsg=4326, allow_override=True)

        gdf = gdf[gdf.geometry.notna()].copy()
        if gdf.empty:
            raise ValueError("The shapefile does not contain usable geometries.")

        try:
            gdf["geometry"] = gdf.geometry.buffer(0)
        except Exception:
            logger.debug("Geometry repair with buffer(0) failed", exc_info=True)

        name_col = _first_existing(gdf.columns, WARD_NAME_CANDIDATES)
        if not name_col:
            raise ValueError("Could not identify a ward name column in the shapefile.")

        code_col = _first_existing(gdf.columns, WARD_CODE_CANDIDATES)
        lga_col = _first_existing(gdf.columns, LGA_CANDIDATES)
        state_col = _first_existing(gdf.columns, STATE_CANDIDATES)

        normalized_names = gdf[name_col].map(_normalize)
        duplicates = normalized_names.duplicated(keep=False)
        gdf["_settlement_duplicated_name"] = duplicates

        ids: List[str] = []
        display_names: List[str] = []
        for idx, row in gdf.iterrows():
            name = str(row.get(name_col) or f"Ward {idx}").strip()
            code = str(row.get(code_col) or "").strip() if code_col else ""
            lga = str(row.get(lga_col) or "").strip() if lga_col else ""
            if code:
                ward_id = _safe_slug(code, fallback=f"ward-{idx}")
            else:
                raw_id = "|".join([_normalize(name), _normalize(lga), str(idx)])
                ward_id = hashlib.sha1(raw_id.encode("utf-8")).hexdigest()[:12]
            display = name if not lga else f"{name} ({lga})"
            ids.append(ward_id)
            display_names.append(display)

        gdf["_settlement_ward_id"] = ids
        gdf["_settlement_display_name"] = display_names
        return gdf, {"name_col": name_col, "code_col": code_col, "lga_col": lga_col, "state_col": state_col}

    def _select_wards(
        self,
        gdf: gpd.GeoDataFrame,
        meta: Dict[str, Optional[str]],
        ward_names: Optional[Sequence[str]],
        ward_ids: Optional[Sequence[str]],
        top_n: Optional[int],
        method: str,
    ) -> Tuple[gpd.GeoDataFrame, str]:
        if ward_ids:
            wanted = {str(value).strip() for value in ward_ids if str(value).strip()}
            selected = gdf[gdf["_settlement_ward_id"].isin(wanted)].copy()
            missing = sorted(wanted - set(selected["_settlement_ward_id"]))
            if missing:
                raise ValueError(f"Unknown ward IDs: {', '.join(missing[:5])}")
            return selected, f"Created classifier for {len(selected)} selected ward(s)."

        if ward_names:
            name_col = meta["name_col"]
            wanted_names = [_normalize(name) for name in ward_names if str(name).strip()]
            matched_indices = []
            ambiguous = []
            missing = []
            for wanted in wanted_names:
                matches = gdf[gdf[name_col].map(_normalize) == wanted]
                if matches.empty:
                    contains = gdf[gdf[name_col].map(_normalize).str.contains(re.escape(wanted), na=False)]
                    matches = contains
                if matches.empty:
                    missing.append(wanted)
                elif len(matches) > 1:
                    ambiguous.append(wanted)
                else:
                    matched_indices.append(matches.index[0])
            if ambiguous:
                raise ValueError(f"Ambiguous ward names: {', '.join(ambiguous)}. Use ward IDs or include LGA context.")
            if missing:
                raise ValueError(f"Ward names not found: {', '.join(missing)}")
            selected = gdf.loc[matched_indices].copy()
            return selected, f"Created classifier for {len(selected)} selected ward(s)."

        if top_n:
            ranking_lookup = self._build_ranking_lookup(method)
            if not ranking_lookup:
                if method == "pca":
                    method = "composite"
                    ranking_lookup = self._build_ranking_lookup(method)
                if not ranking_lookup:
                    raise ValueError("Risk rankings are not available yet. Run malaria risk analysis or select wards manually.")

            ranked_rows = []
            for idx, row in gdf.iterrows():
                rank_info = ranking_lookup.get(str(row["_settlement_ward_id"])) or ranking_lookup.get(_normalize(row.get(meta["name_col"])))
                if rank_info and rank_info.get("rank") is not None:
                    ranked_rows.append((float(rank_info["rank"]), idx))
            ranked_rows.sort(key=lambda item: item[0])
            selected_indices = [idx for _, idx in ranked_rows[: int(top_n)]]
            if not selected_indices:
                raise ValueError("Could not match ranked wards to shapefile boundaries.")
            selected = gdf.loc[selected_indices].copy()
            return selected, f"Created classifier for the top {len(selected)} {method} risk-ranked ward(s)."

        ranking_lookup = self._build_ranking_lookup(method)
        if not ranking_lookup and method == "pca":
            method = "composite"
            ranking_lookup = self._build_ranking_lookup(method)

        if ranking_lookup:
            ranked_rows = []
            for idx, row in gdf.iterrows():
                rank_info = ranking_lookup.get(str(row["_settlement_ward_id"])) or ranking_lookup.get(_normalize(row.get(meta["name_col"])))
                if rank_info and rank_info.get("rank") is not None:
                    ranked_rows.append((float(rank_info["rank"]), idx))
            ranked_rows.sort(key=lambda item: item[0])
            selected_indices = [idx for _, idx in ranked_rows[:10]]
            if selected_indices:
                selected = gdf.loc[selected_indices].copy()
                return selected, f"Created classifier for the top {len(selected)} {method} risk-ranked ward(s)."

        selected = gdf.head(1).copy()
        return selected, "Created classifier for the first available ward. Run risk analysis first to auto-select top-risk wards."

    def _build_grid(self, selected: gpd.GeoDataFrame, cell_size_m: int, max_cells: int) -> gpd.GeoDataFrame:
        metric_crs = selected.estimate_utm_crs()
        if metric_crs is None:
            metric_crs = "EPSG:3857"
        selected_metric = selected.to_crs(metric_crs)
        cells = []

        for _, ward in selected_metric.iterrows():
            geom = ward.geometry
            if geom is None or geom.is_empty:
                continue
            minx, miny, maxx, maxy = geom.bounds
            cols = max(1, math.ceil((maxx - minx) / cell_size_m))
            rows = max(1, math.ceil((maxy - miny) / cell_size_m))
            if len(cells) + (cols * rows) > max_cells * 3:
                raise ValueError("Requested grid is too large. Increase cell size or select fewer wards.")

            cell_index = 0
            for col in range(cols):
                for row in range(rows):
                    x1 = minx + col * cell_size_m
                    y1 = miny + row * cell_size_m
                    candidate = box(x1, y1, min(x1 + cell_size_m, maxx), min(y1 + cell_size_m, maxy))
                    if not candidate.intersects(geom):
                        continue
                    clipped = candidate.intersection(geom)
                    if clipped.is_empty:
                        continue
                    ward_id = str(ward["_settlement_ward_id"])
                    grid_id = f"{ward_id}-{cell_index:04d}"
                    cells.append({
                        "classification_id": "",
                        "grid_id": grid_id,
                        "ward_id": ward_id,
                        "ward_name": ward["_settlement_display_name"],
                        "cell_index": cell_index,
                        "cell_size_m": cell_size_m,
                        "geometry": clipped,
                    })
                    cell_index += 1
                    if len(cells) > max_cells:
                        raise ValueError(f"Grid would create more than {max_cells} cells. Increase cell size or select fewer wards.")

        grid = gpd.GeoDataFrame(cells, geometry="geometry", crs=metric_crs)
        return grid.to_crs(epsg=4326)

    def _load_rankings(self, method: str) -> Optional[pd.DataFrame]:
        filename = "analysis_vulnerability_rankings_pca.csv" if method == "pca" else "analysis_vulnerability_rankings.csv"
        path = self.session_folder / filename
        if not path.exists():
            return None
        try:
            return pd.read_csv(path)
        except Exception:
            logger.warning("Could not load ranking file %s", path, exc_info=True)
            return None

    def _build_ranking_lookup(self, method: str) -> Dict[str, Dict[str, Any]]:
        rankings = self._load_rankings(method)
        if rankings is None or rankings.empty:
            return {}

        code_col = _first_existing(rankings.columns, WARD_CODE_CANDIDATES)
        name_col = _first_existing(rankings.columns, WARD_NAME_CANDIDATES)
        rank_col = _first_existing(rankings.columns, RANK_CANDIDATES)
        if not rank_col:
            numeric_cols = rankings.select_dtypes(include="number").columns.tolist()
            rank_col = numeric_cols[0] if numeric_cols else None
        if not rank_col:
            return {}

        lookup: Dict[str, Dict[str, Any]] = {}
        for _, row in rankings.iterrows():
            try:
                rank_value = float(row.get(rank_col))
            except Exception:
                continue
            info = {"rank": rank_value, "ranking_method": method, "rank_column": rank_col}
            if code_col and pd.notna(row.get(code_col)):
                lookup[_safe_slug(str(row.get(code_col)))] = info
            if name_col and pd.notna(row.get(name_col)):
                lookup[_normalize(row.get(name_col))] = info
        return lookup

    def _new_classification_id(self, selected: gpd.GeoDataFrame, cell_size_m: int) -> str:
        ward_part = "-".join(str(x) for x in selected["_settlement_ward_id"].tolist()[:3])
        digest = hashlib.sha1(f"{ward_part}|{cell_size_m}|{_utc_now()}".encode("utf-8")).hexdigest()[:10]
        return f"settlement-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{digest}"

    def _classification_dir(self, classification_id: str) -> Path:
        safe_id = _safe_slug(classification_id, fallback="classification")
        if safe_id != classification_id:
            raise ValueError("Invalid classification_id.")
        path = (self.settlement_root / safe_id).resolve()
        self._ensure_inside_session(path)
        return path

    def _export_dir(self, classification_id: str) -> Path:
        safe_id = _safe_slug(classification_id, fallback="classification")
        path = (self.export_root / self.session_id / f"settlement_export_{safe_id}").resolve()
        if not str(path).startswith(str((self.export_root / self.session_id).resolve()) + os.path.sep):
            raise ValueError("Invalid export path.")
        return path

    def _ensure_inside_session(self, path: Path) -> None:
        resolved = path.resolve()
        if resolved != self.session_folder and not str(resolved).startswith(str(self.session_folder) + os.path.sep):
            raise ValueError("Path is outside the session folder.")

    def _load_metadata(self, classification_id: str) -> Dict[str, Any]:
        path = self._classification_dir(classification_id) / "metadata.json"
        if not path.exists():
            raise FileNotFoundError("Classification not found.")
        return json.loads(path.read_text(encoding="utf-8"))

    def _touch_metadata(self, classification_id: str) -> None:
        metadata = self._load_metadata(classification_id)
        metadata["updated_at"] = _utc_now()
        self._write_json_atomic(self._classification_dir(classification_id) / "metadata.json", metadata)

    def _valid_grid_ids(self, classification_id: str) -> Dict[str, Dict[str, Any]]:
        grid = gpd.read_file(self._classification_dir(classification_id) / "grid.geojson")
        return {
            str(row["grid_id"]): {"ward_id": row.get("ward_id"), "ward_name": row.get("ward_name")}
            for _, row in grid.drop(columns="geometry", errors="ignore").iterrows()
        }

    def _write_annotations_csv(self, classification_id: str, annotations_doc: Dict[str, Any]) -> Path:
        rows = list(annotations_doc.get("annotations", {}).values())
        path = self._classification_dir(classification_id) / "annotations.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        return path

    def _write_json_atomic(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as tmp:
            json.dump(payload, tmp, indent=2, ensure_ascii=False)
            tmp.write("\n")
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)

    def _download_links(self, classification_id: str) -> List[Dict[str, str]]:
        return [
            {
                "url": f"/export/download/{self.session_id}/settlement_export_{classification_id}/settlement_annotations.csv",
                "filename": "settlement_annotations.csv",
                "description": "Settlement annotations CSV",
                "type": "csv",
            },
            {
                "url": f"/export/download/{self.session_id}/settlement_export_{classification_id}/settlement_classified_grid.geojson",
                "filename": "settlement_classified_grid.geojson",
                "description": "Classified settlement grid GeoJSON",
                "type": "geojson",
            },
        ]

    def _render_selector_html(self, metadata: Dict[str, Any], wards: List[Dict[str, Any]], lgas: List[str]) -> str:
        payload = {
            "sessionId": self.session_id,
            "method": metadata["method"],
            "cellSizeM": metadata["cell_size_m"],
            "includeNoBuildings": metadata["include_no_buildings"],
            "boundariesUrl": f"/api/settlement/{self.session_id}/boundaries?method={metadata['method']}",
            "createUrl": f"/api/settlement/{self.session_id}/classifications",
            "wards": wards,
            "lgas": lgas,
            "wardCount": metadata["ward_count"],
            "lgaCount": metadata["lga_count"],
        }
        config_json = json.dumps(payload, ensure_ascii=False)
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Settlement Classification Selector</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    html, body {{ height: 100%; margin: 0; font-family: Arial, sans-serif; color: #17202a; }}
    #app {{ display: grid; grid-template-columns: minmax(0, 1fr) 360px; height: 840px; background: #f7f9fb; }}
    #map {{ min-height: 760px; background: #dce5ea; }}
    #panel {{ border-left: 1px solid #d0d7de; padding: 14px; overflow-y: auto; background: #fff; }}
    h1 {{ font-size: 18px; margin: 0 0 6px; }}
    h2 {{ font-size: 14px; margin: 18px 0 8px; }}
    .muted {{ color: #57606a; font-size: 13px; line-height: 1.35; }}
    .row {{ margin: 12px 0; }}
    .mode {{ display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }}
    .mode button {{ background: #f6f8fa; color: #24292f; border: 1px solid #d0d7de; }}
    .mode button.active {{ background: #0969da; color: #fff; border-color: #0969da; }}
    label {{ display: block; font-size: 13px; font-weight: 600; margin-bottom: 6px; }}
    select, input, textarea, button {{ width: 100%; box-sizing: border-box; font: inherit; }}
    select, input, textarea {{ border: 1px solid #c9d1d9; border-radius: 6px; padding: 8px; background: #fff; }}
    textarea {{ min-height: 90px; resize: vertical; }}
    button {{ border: 0; border-radius: 6px; padding: 9px 10px; background: #1f6feb; color: white; cursor: pointer; }}
    button.secondary {{ background: #57606a; }}
    button:disabled {{ background: #9aa4b2; cursor: not-allowed; }}
    .selected {{ padding: 8px; background: #f6f8fa; border-radius: 6px; font-size: 13px; }}
    .status {{ font-size: 13px; min-height: 18px; }}
    .hidden {{ display: none; }}
    .legend-item {{ display: flex; align-items: center; gap: 8px; font-size: 13px; margin: 6px 0; }}
    .swatch {{ width: 14px; height: 14px; border-radius: 3px; border: 1px solid rgba(0,0,0,.18); }}
    .boundary-hit {{ background: #eaf5ff; border-radius: 6px; padding: 8px; font-size: 13px; }}
    @media (max-width: 820px) {{ #app {{ grid-template-columns: 1fr; height: auto; }} #map {{ height: 600px; min-height: 600px; }} #panel {{ border-left: 0; border-top: 1px solid #d0d7de; }} }}
  </style>
</head>
<body>
  <div id="app">
    <div id="map"></div>
    <aside id="panel">
      <section id="selectorPanel">
        <h1>Settlement Classification</h1>
        <div class="muted" id="overviewSummary"></div>
        <h2>Focus</h2>
        <div class="mode">
          <button type="button" data-mode="lga" class="active">LGA</button>
          <button type="button" data-mode="ward">Ward</button>
          <button type="button" data-mode="risk">Risk-ranked</button>
          <button type="button" data-mode="map">Map select</button>
        </div>
        <div class="row" id="lgaRow">
          <label for="lgaSelect">LGA</label>
          <select id="lgaSelect"></select>
        </div>
        <div class="row" id="wardRow">
          <label for="wardSelect">Ward</label>
          <select id="wardSelect"></select>
        </div>
        <div class="row hidden" id="riskRow">
          <label for="topInput">Top-ranked wards</label>
          <input id="topInput" type="number" min="1" max="25" value="10">
        </div>
        <div class="row hidden" id="mapRow">
          <div class="boundary-hit" id="mapSelection">Click a ward polygon on the map.</div>
        </div>
        <div class="row">
          <label for="cellSizeInput">Grid size (meters)</label>
          <input id="cellSizeInput" type="number" min="100" max="5000" step="50" value="500">
        </div>
        <div class="row"><button id="generateBtn">Generate Grid</button></div>
      </section>

      <section id="classificationPanel" class="hidden">
        <h1>Classify Grid</h1>
        <div class="muted" id="classificationSummary"></div>
        <div class="row selected" id="selectedCell">Select a grid cell on the map.</div>
        <div class="row">
          <label for="labelSelect">Class</label>
          <select id="labelSelect"></select>
        </div>
        <div class="row">
          <label for="notesInput">Notes</label>
          <textarea id="notesInput" maxlength="1000" placeholder="Visible features, uncertainty, or validation notes"></textarea>
        </div>
        <div class="row"><button id="saveBtn" disabled>Save Classification</button></div>
        <div class="row"><button id="backBtn" class="secondary">Back to Overview</button></div>
        <div class="row"><button id="exportBtn" class="secondary">Refresh Exports</button></div>
        <div class="row status" id="status"></div>
        <div class="row"><strong>Legend</strong><div id="legend"></div></div>
      </section>
    </aside>
  </div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const CONFIG = {config_json};
    const LABELS = ["Formal", "Informal", "Slum", "No Buildings/Avoid Area"];
    const COLORS = {{
      "Formal": "#2b8a3e",
      "Informal": "#f08c00",
      "Slum": "#c92a2a",
      "No Buildings/Avoid Area": "#6c757d"
    }};
    const DEFAULT_COLOR = "#3388ff";
    let mode = "lga";
    let boundariesLayer = null;
    let gridLayer = null;
    let selectedBoundaryId = null;
    let selectedFeature = null;
    let selectedLayer = null;
    let annotations = {{}};
    let currentClassification = null;

    const map = L.map("map", {{ zoomControl: true }});
    const esriImagery = L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}", {{ attribution: "Tiles &copy; Esri" }});
    const nasaBlue = L.tileLayer("https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/BlueMarble_ShadedRelief/default/GoogleMapsCompatible_Level8/{{z}}/{{y}}/{{x}}.jpg", {{ attribution: "Imagery &copy; NASA GIBS", maxZoom: 8 }});
    const osm = L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{ attribution: "&copy; OpenStreetMap contributors" }});
    const carto = L.tileLayer("https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png", {{ attribution: "&copy; OpenStreetMap &copy; CARTO" }});
    esriImagery.addTo(map);
    L.control.layers({{
      "Esri satellite": esriImagery,
      "NASA satellite": nasaBlue,
      "OpenStreetMap": osm,
      "Light reference": carto
    }}, {{}}, {{ collapsed: false }}).addTo(map);

    const lgaSelect = document.getElementById("lgaSelect");
    const wardSelect = document.getElementById("wardSelect");
    const topInput = document.getElementById("topInput");
    const cellSizeInput = document.getElementById("cellSizeInput");
    const generateBtn = document.getElementById("generateBtn");
    const selectorPanel = document.getElementById("selectorPanel");
    const classificationPanel = document.getElementById("classificationPanel");
    const labelSelect = document.getElementById("labelSelect");
    const notesInput = document.getElementById("notesInput");
    const saveBtn = document.getElementById("saveBtn");
    const exportBtn = document.getElementById("exportBtn");
    const backBtn = document.getElementById("backBtn");
    const statusEl = document.getElementById("status");
    const selectedCell = document.getElementById("selectedCell");
    const mapSelection = document.getElementById("mapSelection");

    document.getElementById("overviewSummary").textContent = `${{CONFIG.wardCount}} wards${{CONFIG.lgaCount ? " across " + CONFIG.lgaCount + " LGAs" : ""}}.`;
    LABELS.forEach(label => {{
      const option = document.createElement("option");
      option.value = label;
      option.textContent = label;
      labelSelect.appendChild(option);
    }});
    document.getElementById("legend").innerHTML = LABELS.map(label => `<div class="legend-item"><span class="swatch" style="background:${{COLORS[label] || DEFAULT_COLOR}}"></span>${{label}}</div>`).join("");

    function setStatus(message, isError=false) {{
      statusEl.textContent = message;
      statusEl.style.color = isError ? "#b42318" : "#1f6f43";
    }}

    function populateSelectors() {{
      lgaSelect.innerHTML = '<option value="">Select LGA</option>' + CONFIG.lgas.map(lga => `<option value="${{escapeAttr(lga)}}">${{escapeHtml(lga)}}</option>`).join("");
      updateWardOptions();
    }}

    function updateWardOptions() {{
      const lga = lgaSelect.value;
      const filtered = CONFIG.wards.filter(w => !lga || String(w.lga || "") === lga);
      wardSelect.innerHTML = '<option value="">Select ward</option>' + filtered.map(w => `<option value="${{escapeAttr(w.ward_id)}}">${{escapeHtml(w.display_name)}}</option>`).join("");
      if (boundariesLayer) boundariesLayer.setStyle(boundaryStyle);
    }}

    function escapeHtml(value) {{
      return String(value ?? "").replace(/[&<>"']/g, c => ({{ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }}[c]));
    }}

    function escapeAttr(value) {{
      return escapeHtml(value).replace(/`/g, "&#96;");
    }}

    function boundaryStyle(feature) {{
      const props = feature.properties || {{}};
      const lga = lgaSelect.value;
      const ward = wardSelect.value;
      const isSelected = selectedBoundaryId && props.ward_id === selectedBoundaryId;
      const inLga = !lga || String(props.lga || "") === lga;
      const isWard = ward && props.ward_id === ward;
      const hasRank = props.rank !== null && props.rank !== undefined;
      return {{
        color: isSelected || isWard ? "#d9480f" : hasRank ? "#7048e8" : "#0969da",
        weight: isSelected || isWard ? 3 : inLga ? 1.4 : 0.7,
        fillColor: hasRank ? "#7048e8" : "#74c0fc",
        fillOpacity: isSelected || isWard ? 0.32 : inLga ? 0.12 : 0.03
      }};
    }}

    function onBoundary(feature, layer) {{
      const props = feature.properties || {{}};
      layer.bindTooltip(props.display_name || props.ward_name || "Ward", {{ sticky: true }});
      layer.on("click", () => {{
        selectedBoundaryId = props.ward_id;
        wardSelect.value = props.ward_id;
        if (props.lga) {{
          lgaSelect.value = props.lga;
          updateWardOptions();
          wardSelect.value = props.ward_id;
        }}
        mapSelection.textContent = props.display_name || props.ward_name || props.ward_id;
        document.querySelector('[data-mode="map"]').click();
        boundariesLayer.setStyle(boundaryStyle);
      }});
    }}

    async function loadOverview() {{
      populateSelectors();
      const response = await fetch(CONFIG.boundariesUrl);
      if (!response.ok) throw new Error("Could not load settlement boundaries");
      const geojson = await response.json();
      boundariesLayer = L.geoJSON(geojson, {{ style: boundaryStyle, onEachFeature: onBoundary }}).addTo(map);
      map.fitBounds(boundariesLayer.getBounds(), {{ padding: [16, 16] }});
    }}

    document.querySelectorAll("[data-mode]").forEach(button => {{
      button.addEventListener("click", () => {{
        mode = button.dataset.mode;
        document.querySelectorAll("[data-mode]").forEach(b => b.classList.toggle("active", b === button));
        document.getElementById("lgaRow").classList.toggle("hidden", !["lga", "ward"].includes(mode));
        document.getElementById("wardRow").classList.toggle("hidden", !["ward"].includes(mode));
        document.getElementById("riskRow").classList.toggle("hidden", mode !== "risk");
        document.getElementById("mapRow").classList.toggle("hidden", mode !== "map");
      }});
    }});
    lgaSelect.addEventListener("change", updateWardOptions);
    wardSelect.addEventListener("change", () => {{
      selectedBoundaryId = wardSelect.value;
      if (boundariesLayer) boundariesLayer.setStyle(boundaryStyle);
    }});

    function buildCreatePayload() {{
      const payload = {{
        method: CONFIG.method,
        cell_size_m: Number(cellSizeInput.value || CONFIG.cellSizeM),
        include_no_buildings: CONFIG.includeNoBuildings
      }};
      if (mode === "risk") {{
        payload.top_n = Number(topInput.value || 10);
      }} else if (mode === "lga") {{
        const lga = lgaSelect.value;
        if (!lga) throw new Error("Select an LGA first.");
        payload.ward_ids = CONFIG.wards.filter(w => String(w.lga || "") === lga).map(w => w.ward_id);
      }} else {{
        const wardId = wardSelect.value || selectedBoundaryId;
        if (!wardId) throw new Error("Select a ward first.");
        payload.ward_ids = [wardId];
      }}
      return payload;
    }}

    generateBtn.addEventListener("click", async () => {{
      try {{
        generateBtn.disabled = true;
        generateBtn.textContent = "Generating...";
        const response = await fetch(CONFIG.createUrl, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(buildCreatePayload())
        }});
        const data = await response.json();
        if (!response.ok || !data.success) throw new Error(data.message || "Grid generation failed");
        await loadClassification(data);
      }} catch (err) {{
        alert(err.message);
      }} finally {{
        generateBtn.disabled = false;
        generateBtn.textContent = "Generate Grid";
      }}
    }});

    function gridStyle(feature) {{
      const annotation = annotations[feature.properties.grid_id];
      const label = annotation && annotation.label;
      return {{
        color: label ? (COLORS[label] || DEFAULT_COLOR) : DEFAULT_COLOR,
        fillColor: label ? (COLORS[label] || DEFAULT_COLOR) : DEFAULT_COLOR,
        weight: selectedFeature && selectedFeature.properties.grid_id === feature.properties.grid_id ? 3 : 1,
        fillOpacity: label ? 0.45 : 0.12
      }};
    }}

    function onGrid(feature, layer) {{
      layer.on("click", () => {{
        selectedFeature = feature;
        selectedLayer = layer;
        const ann = annotations[feature.properties.grid_id] || {{}};
        labelSelect.value = ann.label || LABELS[0];
        notesInput.value = ann.notes || "";
        selectedCell.textContent = `${{feature.properties.ward_name}} | ${{feature.properties.grid_id}}`;
        saveBtn.disabled = false;
        gridLayer.setStyle(gridStyle);
      }});
    }}

    async function loadClassification(data) {{
      currentClassification = data;
      selectorPanel.classList.add("hidden");
      classificationPanel.classList.remove("hidden");
      document.getElementById("classificationSummary").textContent = `${{data.message || "Grid ready."}} ${{data.grid_cell_count}} grid cells.`;
      if (boundariesLayer) map.removeLayer(boundariesLayer);
      if (gridLayer) map.removeLayer(gridLayer);
      const gridUrl = `/api/settlement/${{CONFIG.sessionId}}/classifications/${{data.classification_id}}/grid`;
      const annUrl = `/api/settlement/${{CONFIG.sessionId}}/classifications/${{data.classification_id}}/annotations`;
      const [gridRes, annRes] = await Promise.all([fetch(gridUrl), fetch(annUrl)]);
      if (!gridRes.ok || !annRes.ok) throw new Error("Could not load generated grid");
      const grid = await gridRes.json();
      const annDoc = await annRes.json();
      annotations = annDoc.annotations || {{}};
      gridLayer = L.geoJSON(grid, {{ style: gridStyle, onEachFeature: onGrid }}).addTo(map);
      map.fitBounds(gridLayer.getBounds(), {{ padding: [20, 20] }});
      setStatus("Ready");
    }}

    saveBtn.addEventListener("click", async () => {{
      if (!selectedFeature || !currentClassification) return;
      saveBtn.disabled = true;
      setStatus("Saving...");
      const response = await fetch(`/api/settlement/${{CONFIG.sessionId}}/classifications/${{currentClassification.classification_id}}/annotations`, {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
        body: JSON.stringify({{
          grid_id: selectedFeature.properties.grid_id,
          label: labelSelect.value,
          notes: notesInput.value
        }})
      }});
      const data = await response.json();
      if (!response.ok || !data.success) {{
        saveBtn.disabled = false;
        setStatus(data.message || "Save failed", true);
        return;
      }}
      annotations[selectedFeature.properties.grid_id] = data.annotation;
      gridLayer.setStyle(gridStyle);
      saveBtn.disabled = false;
      setStatus(`Saved. ${{data.count}} classified cell(s).`);
    }});

    exportBtn.addEventListener("click", async () => {{
      if (!currentClassification) return;
      setStatus("Refreshing exports...");
      const response = await fetch(`/api/settlement/${{CONFIG.sessionId}}/classifications/${{currentClassification.classification_id}}/export`, {{ method: "POST" }});
      const data = await response.json();
      setStatus(response.ok && data.success ? "Exports refreshed." : (data.message || "Export failed"), !response.ok);
    }});

    backBtn.addEventListener("click", async () => {{
      classificationPanel.classList.add("hidden");
      selectorPanel.classList.remove("hidden");
      selectedFeature = null;
      selectedLayer = null;
      saveBtn.disabled = true;
      if (gridLayer) map.removeLayer(gridLayer);
      await loadOverview();
    }});

    loadOverview().catch(err => alert(err.message));
  </script>
</body>
</html>
"""

    def _render_classifier_html(self, metadata: Dict[str, Any]) -> str:
        payload = {
            "sessionId": self.session_id,
            "classificationId": metadata["classification_id"],
            "labels": metadata["labels"],
            "gridUrl": f"/api/settlement/{self.session_id}/classifications/{metadata['classification_id']}/grid",
            "annotationsUrl": f"/api/settlement/{self.session_id}/classifications/{metadata['classification_id']}/annotations",
            "exportUrl": f"/api/settlement/{self.session_id}/classifications/{metadata['classification_id']}/export",
            "wards": metadata["wards"],
            "gridCellCount": metadata["grid_cell_count"],
            "selectionMessage": metadata["selection_message"],
        }
        config_json = json.dumps(payload, ensure_ascii=False)
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Settlement Classification</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    html, body {{ height: 100%; margin: 0; font-family: Arial, sans-serif; color: #1f2933; }}
    #app {{ display: grid; grid-template-columns: minmax(0, 1fr) 320px; height: 780px; }}
    #map {{ min-height: 680px; }}
    #panel {{ border-left: 1px solid #d8dee4; padding: 14px; overflow-y: auto; background: #ffffff; }}
    h1 {{ font-size: 18px; margin: 0 0 8px; }}
    .muted {{ color: #5f6b7a; font-size: 13px; line-height: 1.35; }}
    .row {{ margin: 12px 0; }}
    label {{ display: block; font-size: 13px; font-weight: 600; margin-bottom: 6px; }}
    select, textarea, button {{ width: 100%; box-sizing: border-box; font: inherit; }}
    select, textarea {{ border: 1px solid #c9d1d9; border-radius: 6px; padding: 8px; }}
    textarea {{ min-height: 90px; resize: vertical; }}
    button {{ border: 0; border-radius: 6px; padding: 9px 10px; background: #1f6feb; color: white; cursor: pointer; }}
    button.secondary {{ background: #57606a; }}
    button:disabled {{ background: #9aa4b2; cursor: not-allowed; }}
    .legend-item {{ display: flex; align-items: center; gap: 8px; font-size: 13px; margin: 6px 0; }}
    .swatch {{ width: 14px; height: 14px; border-radius: 3px; border: 1px solid rgba(0,0,0,.18); }}
    .status {{ font-size: 13px; min-height: 18px; }}
    .selected {{ padding: 8px; background: #f6f8fa; border-radius: 6px; font-size: 13px; }}
    @media (max-width: 760px) {{ #app {{ grid-template-columns: 1fr; height: auto; }} #map {{ height: 560px; }} #panel {{ border-left: 0; border-top: 1px solid #d8dee4; }} }}
  </style>
</head>
<body>
  <div id="app">
    <div id="map"></div>
    <aside id="panel">
      <h1>Settlement Classification</h1>
      <div class="muted" id="summary"></div>
      <div class="row selected" id="selectedCell">Select a grid cell on the map.</div>
      <div class="row">
        <label for="labelSelect">Class</label>
        <select id="labelSelect"></select>
      </div>
      <div class="row">
        <label for="notesInput">Notes</label>
        <textarea id="notesInput" maxlength="1000" placeholder="Visible features, uncertainty, or validation notes"></textarea>
      </div>
      <div class="row"><button id="saveBtn" disabled>Save Classification</button></div>
      <div class="row"><button id="exportBtn" class="secondary">Refresh Exports</button></div>
      <div class="row status" id="status"></div>
      <div class="row"><strong>Legend</strong><div id="legend"></div></div>
    </aside>
  </div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const CONFIG = {config_json};
    const COLORS = {{
      "Formal": "#2b8a3e",
      "Informal": "#f08c00",
      "Slum": "#c92a2a",
      "No Buildings/Avoid Area": "#6c757d"
    }};
    const DEFAULT_COLOR = "#3388ff";
    let selectedLayer = null;
    let selectedFeature = null;
    let annotations = {{}};

    const map = L.map("map").setView([9.1, 8.7], 8);
    L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}", {{
      attribution: "Tiles &copy; Esri"
    }}).addTo(map);
    L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
      attribution: "&copy; OpenStreetMap contributors",
      opacity: 0.35
    }}).addTo(map);

    const labelSelect = document.getElementById("labelSelect");
    const notesInput = document.getElementById("notesInput");
    const saveBtn = document.getElementById("saveBtn");
    const exportBtn = document.getElementById("exportBtn");
    const statusEl = document.getElementById("status");
    const selectedCell = document.getElementById("selectedCell");

    CONFIG.labels.forEach(label => {{
      const option = document.createElement("option");
      option.value = label;
      option.textContent = label;
      labelSelect.appendChild(option);
    }});
    document.getElementById("summary").textContent = `${{CONFIG.selectionMessage}} ${{CONFIG.gridCellCount}} grid cells.`;
    document.getElementById("legend").innerHTML = CONFIG.labels.map(label => `<div class="legend-item"><span class="swatch" style="background:${{COLORS[label] || DEFAULT_COLOR}}"></span>${{label}}</div>`).join("");

    function setStatus(message, isError=false) {{
      statusEl.textContent = message;
      statusEl.style.color = isError ? "#b42318" : "#1f6f43";
    }}

    function styleFeature(feature) {{
      const annotation = annotations[feature.properties.grid_id];
      const label = annotation && annotation.label;
      return {{
        color: label ? (COLORS[label] || DEFAULT_COLOR) : DEFAULT_COLOR,
        fillColor: label ? (COLORS[label] || DEFAULT_COLOR) : DEFAULT_COLOR,
        weight: selectedFeature && selectedFeature.properties.grid_id === feature.properties.grid_id ? 3 : 1,
        fillOpacity: label ? 0.45 : 0.12
      }};
    }}

    function onEachFeature(feature, layer) {{
      layer.on("click", () => {{
        selectedFeature = feature;
        selectedLayer = layer;
        const ann = annotations[feature.properties.grid_id] || {{}};
        labelSelect.value = ann.label || CONFIG.labels[0];
        notesInput.value = ann.notes || "";
        selectedCell.textContent = `${{feature.properties.ward_name}} | ${{feature.properties.grid_id}}`;
        saveBtn.disabled = false;
        gridLayer.setStyle(styleFeature);
      }});
    }}

    let gridLayer = null;
    async function loadMap() {{
      const [gridRes, annRes] = await Promise.all([fetch(CONFIG.gridUrl), fetch(CONFIG.annotationsUrl)]);
      if (!gridRes.ok) throw new Error("Could not load grid");
      if (!annRes.ok) throw new Error("Could not load annotations");
      const grid = await gridRes.json();
      const annDoc = await annRes.json();
      annotations = annDoc.annotations || {{}};
      gridLayer = L.geoJSON(grid, {{ style: styleFeature, onEachFeature }}).addTo(map);
      map.fitBounds(gridLayer.getBounds(), {{ padding: [20, 20] }});
      setStatus("Ready");
    }}

    saveBtn.addEventListener("click", async () => {{
      if (!selectedFeature) return;
      saveBtn.disabled = true;
      setStatus("Saving...");
      const response = await fetch(CONFIG.annotationsUrl, {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
        body: JSON.stringify({{
          grid_id: selectedFeature.properties.grid_id,
          label: labelSelect.value,
          notes: notesInput.value
        }})
      }});
      const data = await response.json();
      if (!response.ok || !data.success) {{
        saveBtn.disabled = false;
        setStatus(data.message || "Save failed", true);
        return;
      }}
      annotations[selectedFeature.properties.grid_id] = data.annotation;
      gridLayer.setStyle(styleFeature);
      saveBtn.disabled = false;
      setStatus(`Saved. ${{data.count}} classified cell(s).`);
    }});

    exportBtn.addEventListener("click", async () => {{
      setStatus("Refreshing exports...");
      const response = await fetch(CONFIG.exportUrl, {{ method: "POST" }});
      const data = await response.json();
      setStatus(response.ok && data.success ? "Exports refreshed." : (data.message || "Export failed"), !response.ok);
    }});

    loadMap().catch(err => setStatus(err.message, true));
  </script>
</body>
</html>
"""


class SettlementClassificationTool(BaseTool):
    """Create a manual grid-based settlement classification map."""

    ward_names: Optional[List[str]] = Field(None, description="Ward names to classify")
    ward_ids: Optional[List[str]] = Field(None, description="Stable ward IDs to classify")
    top_n: Optional[int] = Field(None, description="Top N risk-ranked wards to classify")
    method: str = Field("composite", description="Ranking method: composite or pca")
    cell_size_m: int = Field(500, description="Grid cell size in meters")
    include_no_buildings: bool = Field(True, description="Include No Buildings/Avoid Area label")

    @classmethod
    def get_tool_name(cls) -> str:
        return "create_settlement_classification"

    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.SETTLEMENT_TOOLS

    @classmethod
    def get_description(cls) -> str:
        return "Create a manual settlement classification grid over selected wards"

    @validator("method")
    def validate_method(cls, value: str) -> str:
        value = (value or "composite").lower()
        if value not in {"composite", "pca"}:
            raise ValueError("method must be composite or pca")
        return value

    def execute(self, session_id: str) -> ToolExecutionResult:
        try:
            service = SettlementClassificationService(session_id)
            if not self.ward_names and not self.ward_ids and self.top_n is None:
                result = service.create_selector_map(
                    method=self.method,
                    cell_size_m=self.cell_size_m,
                    include_no_buildings=self.include_no_buildings,
                )
                message = (
                    f"Settlement classification selector opened with {result['ward_count']} wards. "
                    "Choose an LGA, ward, risk-ranked set, or map selection, then generate the grid."
                )
            else:
                result = service.create_classification(
                    ward_names=self.ward_names,
                    ward_ids=self.ward_ids,
                    top_n=self.top_n,
                    method=self.method,
                    cell_size_m=self.cell_size_m,
                    include_no_buildings=self.include_no_buildings,
                )
                message = (
                    f"Settlement classification map created for {len(result['selected_wards'])} ward(s) "
                    f"with {result['grid_cell_count']} grid cells. Use the map to classify cells and add notes."
                )
            return self._create_success_result(message=message, data=result, web_path=result.get("web_path"))
        except Exception as exc:
            logger.error("Settlement classification failed: %s", exc, exc_info=True)
            return self._create_error_result(str(exc))
