"""ITN Population Data Loader

This loader now reads the unified population CSV for all Nigerian wards and
merges it with ward metadata from the authoritative shapefile so that every
state and ward has a consistent WardCode, LGA information, and coordinates.
"""

import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)


class ITNPopulationLoader:
    """Loader for ITN population data sourced from the national ward dataset."""

    def __init__(self) -> None:
        self.base_path = Path(__file__).resolve().parents[2]
        self.population_csv_path = self.base_path / "www" / "wards_with_pop.csv"
        self.shapefile_path = self.base_path / "www" / "complete_names_wards" / "wards.shp"

        self._state_code_to_name: Dict[str, str] = {}
        self._state_name_to_code: Dict[str, str] = {}
        self._state_name_aliases: Dict[str, str] = {}
        self._master_df: Optional[pd.DataFrame] = None
        self._cache: Dict[str, pd.DataFrame] = {}

    def _load_master_dataset(self) -> Optional[pd.DataFrame]:
        """Load and cache the merged population dataset for all wards."""
        if self._master_df is not None:
            return self._master_df

        if not self.population_csv_path.exists():
            logger.error(f"Population CSV not found at {self.population_csv_path}")
            return None

        try:
            population_df = pd.read_csv(self.population_csv_path)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(f"Failed to read population CSV: {exc}")
            return None

        required_columns = {"StateCode", "LGACode", "WardCode", "WardName", "Population"}
        missing_columns = required_columns.difference(population_df.columns)
        if missing_columns:
            logger.error(
                "Population CSV is missing required columns: %s",
                ", ".join(sorted(missing_columns)),
            )
            return None

        if not self.shapefile_path.exists():
            logger.error(f"Ward shapefile not found at {self.shapefile_path}")
            return None

        try:
            wards_gdf = gpd.read_file(self.shapefile_path)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(f"Failed to read ward shapefile: {exc}")
            return None

        # Ensure WardCode is string and standardized for merging
        population_df["WardCode"] = population_df["WardCode"].astype(str).str.strip()
        wards_gdf["WardCode"] = wards_gdf["WardCode"].astype(str).str.strip()

        # Compute centroid coordinates for reference (EPSG:4326)
        try:
            wards_gdf = wards_gdf.to_crs(epsg=4326)
            centroids = wards_gdf.geometry.centroid
            wards_gdf["AvgLatitude"] = centroids.y
            wards_gdf["AvgLongitude"] = centroids.x
        except Exception as exc:  # pragma: no cover - fallback without coordinates
            logger.warning(f"Could not compute ward centroids: {exc}")
            wards_gdf["AvgLatitude"] = pd.NA
            wards_gdf["AvgLongitude"] = pd.NA

        shapefile_columns = [
            "WardCode",
            "StateCode",
            "StateName",
            "LGAName",
            "LGACode",
            "Urban",
            "AvgLatitude",
            "AvgLongitude",
        ]
        ward_metadata = wards_gdf[shapefile_columns].copy()

        merged = population_df.merge(ward_metadata, on="WardCode", how="left", suffixes=("", "_shp"))

        # Backfill identifiers from the shapefile for robustness
        for column in ["StateCode", "LGACode"]:
            shapefile_column = f"{column}_shp"
            if shapefile_column in merged.columns:
                merged[column] = merged[column].fillna(merged[shapefile_column])

        # Handle StateName backfill - only if column exists
        if "StateName" not in merged.columns:
            if "StateName_shp" in merged.columns:
                merged["StateName"] = merged["StateName_shp"]
        else:
            if "StateName_shp" in merged.columns:
                merged["StateName"] = merged["StateName"].fillna(merged["StateName_shp"])

        # Handle LGAName backfill - only if column exists
        if "LGAName_shp" in merged.columns:
            merged["LGAName"] = merged["LGAName"].fillna(merged["LGAName_shp"])
        merged["AdminLevel2"] = merged["LGAName"].fillna(merged["LGACode"])

        # Populate lowercase ward names for fuzzy matching fallbacks
        merged["WardName_lower"] = merged["WardName"].astype(str).str.strip().str.lower()

        # Build state lookup dictionaries
        state_info = (
            merged[["StateCode", "StateName"]]
            .dropna(subset=["StateCode"])
            .drop_duplicates()
            .sort_values("StateCode")
        )

        self._state_code_to_name = {
            str(row.StateCode).strip().upper(): str(row.StateName).strip()
            for _, row in state_info.iterrows()
            if str(row.StateCode).strip()
        }

        self._state_name_to_code = {
            str(name).strip().lower(): code
            for code, name in self._state_code_to_name.items()
        }

        # Add "State" suffix aliases for robustness (e.g., "Kaduna State")
        self._state_name_aliases = {
            f"{name.lower()} state": code for code, name in self._state_code_to_name.items()
        }
        self._state_name_aliases.update({"fct": "FC", "abuja": "FC"})

        # Drop helper columns from merge
        drop_columns = [col for col in merged.columns if col.endswith("_shp")]
        if drop_columns:
            merged = merged.drop(columns=drop_columns)

        # Ensure population is numeric and drop rows without WardCode
        merged["Population"] = pd.to_numeric(merged["Population"], errors="coerce")
        merged = merged.dropna(subset=["WardCode", "Population"])

        self._master_df = merged
        logger.info(
            "Loaded national ITN population dataset with %d wards across %d states",
            len(merged),
            len(self._state_code_to_name),
        )
        return self._master_df

    def _resolve_state_code(self, state_identifier: str) -> Optional[str]:
        """Resolve a provided state name or code to the canonical two-letter code."""
        if not state_identifier:
            return None

        state_identifier = str(state_identifier).strip()
        if not state_identifier:
            return None

        identifier_upper = state_identifier.upper()
        identifier_lower = state_identifier.lower()

        # Direct code match
        if len(identifier_upper) == 2 and identifier_upper in self._state_code_to_name:
            return identifier_upper

        # Exact name match
        if identifier_lower in self._state_name_to_code:
            return self._state_name_to_code[identifier_lower]

        # Remove trailing "state" for common input variations
        if identifier_lower.endswith(" state"):
            trimmed = identifier_lower[:-6].strip()
            if trimmed in self._state_name_to_code:
                return self._state_name_to_code[trimmed]

        # Alias lookup (includes Abuja/FCT variations)
        if identifier_lower in self._state_name_aliases:
            return self._state_name_aliases[identifier_lower]

        return None

    @lru_cache(maxsize=1)
    def get_available_states(self) -> List[str]:
        """Return the list of state names that have population data."""
        master = self._load_master_dataset()
        if master is None:
            return []
        return sorted({name for name in self._state_code_to_name.values() if name})

    def get_state_code_map(self) -> Dict[str, str]:
        """Return a mapping from state code to state name."""
        self._load_master_dataset()
        return self._state_code_to_name.copy()

    def load_state_population(self, state_name: str) -> Optional[pd.DataFrame]:
        """Return population data for the requested state."""
        master = self._load_master_dataset()
        if master is None:
            return None

        state_code = self._resolve_state_code(state_name)
        if not state_code:
            logger.warning(f"State identifier '{state_name}' could not be resolved")
            return None

        cache_key = state_code
        if cache_key in self._cache:
            return self._cache[cache_key].copy()

        state_df = master[master["StateCode"].str.upper() == state_code].copy()
        if state_df.empty:
            logger.warning(f"No population records found for state '{state_name}' (code {state_code})")
            return None

        state_df["StateName"] = state_df["StateName"].fillna(self._state_code_to_name.get(state_code))
        logger.info(
            "Loaded %d wards for %s (%s) with total population %s",
            len(state_df),
            state_df["StateName"].iloc[0] if not state_df["StateName"].isna().all() else state_code,
            state_code,
            f"{state_df['Population'].sum():,.0f}",
        )

        # Ensure expected helper columns exist
        if "AdminLevel2" not in state_df.columns:
            state_df["AdminLevel2"] = state_df["LGAName"].fillna(state_df["LGACode"])
        if "AvgLatitude" not in state_df.columns:
            state_df["AvgLatitude"] = pd.NA
        if "AvgLongitude" not in state_df.columns:
            state_df["AvgLongitude"] = pd.NA
        if "WardName_lower" not in state_df.columns:
            state_df["WardName_lower"] = state_df["WardName"].astype(str).str.lower()

        self._cache[cache_key] = state_df
        return state_df.copy()

    def get_ward_populations(self, state_name: str, ward_names: List[str] = None) -> Dict[str, int]:
        """Return a ward -> population mapping for the requested state."""
        df = self.load_state_population(state_name)
        if df is None:
            return {}

        if ward_names is None:
            return {row.WardName: int(row.Population) for _, row in df.iterrows()}

        ward_names_lower = {name.lower() for name in ward_names}
        filtered = df[df["WardName_lower"].isin(ward_names_lower)]
        return {row.WardName: int(row.Population) for _, row in filtered.iterrows()}

    def get_total_population(self, state_name: str) -> int:
        """Return total population for the requested state."""
        df = self.load_state_population(state_name)
        if df is None:
            return 0
        return int(df["Population"].sum())

    def match_ward_names(self, state_name: str, input_ward_names: List[str]) -> Dict[str, str]:
        """Match provided ward names to canonical names for the specified state."""
        df = self.load_state_population(state_name)
        if df is None:
            return {}

        standard_lookup = {row.WardName_lower: row.WardName for _, row in df.iterrows()}
        mapping: Dict[str, str] = {}

        for input_ward in input_ward_names:
            normalized = str(input_ward).strip().lower()
            if not normalized:
                continue
            if normalized in standard_lookup:
                mapping[input_ward] = standard_lookup[normalized]
            else:
                logger.warning(f"No match found for ward: {input_ward}")

        return mapping


# Singleton instance
_loader_instance: Optional[ITNPopulationLoader] = None


def get_population_loader() -> ITNPopulationLoader:
    """Return the singleton population loader."""
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = ITNPopulationLoader()
    return _loader_instance
