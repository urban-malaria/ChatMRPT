"""Downloadable data products for the malaria burden workflow."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd

logger = logging.getLogger(__name__)


DISPLAY_NAMES: Dict[str, Dict[str, str]] = {
    "WardCode": {"display_name": "Ward code", "definition": "Unique ward identifier from the boundary dataset.", "unit": ""},
    "StateCode": {"display_name": "State code", "definition": "State identifier from the boundary dataset.", "unit": ""},
    "LGACode": {"display_name": "LGA code", "definition": "Local Government Area identifier from the boundary dataset.", "unit": ""},
    "WardName": {"display_name": "Ward name", "definition": "Ward name from the boundary dataset.", "unit": ""},
    "LGA": {"display_name": "Local Government Area", "definition": "Local Government Area name.", "unit": ""},
    "State": {"display_name": "State", "definition": "Nigerian state name.", "unit": ""},
    "GeopoliticalZone": {"display_name": "Geopolitical zone", "definition": "Nigerian geopolitical zone used to choose environmental variables.", "unit": ""},
    "Burden": {"display_name": "Malaria burden", "definition": "Positive malaria tests divided by the selected ward population denominator, multiplied by 1,000.", "unit": "cases per 1,000 population"},
    "Population": {"display_name": "Selected population denominator", "definition": "Population denominator used for the current burden calculation.", "unit": "people"},
    "Total_Positive": {"display_name": "Total positive malaria tests", "definition": "Positive tests aggregated for the selected facility level and age group.", "unit": "tests"},
    "rainfall": {"display_name": "Rainfall", "definition": "Rainfall summary extracted for the ward.", "unit": ""},
    "ndvi": {"display_name": "Normalized Difference Vegetation Index", "definition": "Vegetation index extracted for the ward.", "unit": "index"},
    "ndwi": {"display_name": "Normalized Difference Water Index", "definition": "Water index extracted for the ward.", "unit": "index"},
    "ndmi": {"display_name": "Normalized Difference Moisture Index", "definition": "Moisture index extracted for the ward.", "unit": "index"},
    "housing_quality": {"display_name": "Housing quality", "definition": "Housing quality surface summarized for the ward.", "unit": "index"},
    "HousingQuality": {"display_name": "Housing quality", "definition": "Housing quality surface summarized for the ward.", "unit": "index"},
    "elevation": {"display_name": "Elevation", "definition": "Elevation summarized for the ward.", "unit": "meters"},
    "urban_extent": {"display_name": "Urban extent", "definition": "Urban extent indicator summarized for the ward.", "unit": ""},
    "urban_percentage": {"display_name": "Urban percentage", "definition": "Share of the ward classified as urban.", "unit": "percent"},
    "NighttimeLights": {"display_name": "Nighttime lights", "definition": "Nighttime lights intensity summarized for the ward.", "unit": "index"},
    "SoilWetness": {"display_name": "Soil wetness", "definition": "Soil wetness indicator summarized for the ward.", "unit": "index"},
    "DistanceToWaterbodies": {"display_name": "Distance to waterbodies", "definition": "Distance from ward area to waterbody features.", "unit": ""},
    "area_km2": {"display_name": "Ward area", "definition": "Ward area.", "unit": "square kilometers"},
    "centroid_lat": {"display_name": "Ward centroid latitude", "definition": "Latitude of the ward centroid.", "unit": "degrees"},
    "centroid_lon": {"display_name": "Ward centroid longitude", "definition": "Longitude of the ward centroid.", "unit": "degrees"},
    "perimeter_km": {"display_name": "Ward perimeter", "definition": "Ward boundary perimeter.", "unit": "kilometers"},
}


def _lookup(column: str) -> Dict[str, str]:
    if column in DISPLAY_NAMES:
        return DISPLAY_NAMES[column]
    lower = column.lower()
    if lower in DISPLAY_NAMES:
        return DISPLAY_NAMES[lower]
    display = column.replace("_", " ").strip().title()
    return {
        "display_name": display,
        "definition": f"Analysis variable: {display}.",
        "unit": "",
    }


def write_data_dictionary(
    df: pd.DataFrame,
    output_path: str | Path,
    *,
    source: str = "ChatMRPT malaria burden workflow",
) -> Path:
    """Write a data dictionary with full display names for analysis variables."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for column in df.columns:
        meta = _lookup(str(column))
        rows.append({
            "raw_column": column,
            "display_name": meta.get("display_name", column),
            "definition": meta.get("definition", ""),
            "unit": meta.get("unit", ""),
            "source": source,
            "derivation": meta.get("derivation", ""),
            "notes": meta.get("notes", ""),
        })
    pd.DataFrame(rows).to_csv(output, index=False)
    return output


def write_population_denominators(
    ward_gdf,
    output_path: str | Path,
    *,
    state_name: str = "",
) -> Optional[Path]:
    """Export population denominators used by the burden workflow."""
    try:
        from app.tpr.utils import extract_ward_population
    except Exception as exc:
        logger.warning("Population denominator export unavailable: %s", exc)
        return None

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    gdf = ward_gdf.copy()
    if gdf.empty:
        return None

    def pick(candidates: Iterable[str]) -> Optional[str]:
        return next((col for col in candidates if col in gdf.columns), None)

    ward_col = pick(["WardName", "Ward", "ADM3_EN"])
    lga_col = pick(["LGAName", "LGA", "ADM2_EN"])
    ward_code_col = pick(["WardCode", "ward_code", "ADM3_PCODE"])
    lga_code_col = pick(["LGACode", "lga_code", "ADM2_PCODE"])
    state_code_col = pick(["StateCode", "state_code", "ADM1_PCODE"])

    records = pd.DataFrame({
        "state": gdf.get("StateName", state_name),
        "state_code": gdf[state_code_col] if state_code_col else "",
        "lga": gdf[lga_col] if lga_col else "",
        "lga_code": gdf[lga_code_col] if lga_code_col else "",
        "ward": gdf[ward_col] if ward_col else "",
        "ward_code": gdf[ward_code_col] if ward_code_col else "",
    })

    denominator_defs = {
        "population_total": ("all_ages", "WorldPop total population"),
        "population_under_5": ("u5", "WorldPop under-5 population"),
        "population_over_5": ("o5", "Derived as total population minus under-5 population"),
        "population_women_15_49": ("pw", "WorldPop women 15-49 population"),
    }
    source_notes: Dict[str, str] = {}
    for column, (age_group, note) in denominator_defs.items():
        series = extract_ward_population(gdf, age_group)
        if series is not None:
            records[column] = pd.to_numeric(series, errors="coerce").round(0).astype("Int64")
            source_notes[column] = note

    if len(records.columns) <= 6:
        return None

    records.to_csv(output, index=False)
    metadata_path = output.with_name("population_data_metadata.json")
    metadata_path.write_text(json.dumps({
        "state": state_name,
        "source": "Population rasters used by ChatMRPT malaria burden calculation",
        "columns": source_notes,
    }, indent=2), encoding="utf-8")
    return output
