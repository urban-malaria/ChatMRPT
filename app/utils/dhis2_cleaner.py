"""
DHIS2 Data Cleaner

Detects and fixes data quality issues specific to DHIS2 pivot table exports:
1. Duplicate column headers (e.g., two columns both named
   "Persons presenting with fever & tested by RDT <5yrs") — merged via sum
2. Mojibake in column names (e.g., period0me → periodname)

Runs conservatively: only applies to files detected as DHIS2 exports.
Non-DHIS2 files pass through unchanged.

See: project/planning/dhis2_cleaner_plan.md (v4)
"""

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.utils.dhis2_mojibake_patterns import MOJIBAKE_PATTERNS

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  Constants
# --------------------------------------------------------------------------- #

# Known intermediate files produced by our TPR/risk analysis pipeline.
# These are NOT user uploads and should be excluded from raw-file fallbacks.
INTERMEDIATE_FILES = frozenset({
    'uploaded_data.csv',
    'raw_data.csv',
    'tpr_results.csv',
    'tpr_time_series.csv',
    'unified_dataset.csv',
    'analysis_cleaned_data.csv',
    'analysis_composite_scores.csv',
    'analysis_normalized_data.csv',
    'analysis_vulnerability_rankings.csv',
    'composite_scores.csv',
    'model_formulas.csv',
})

# Malaria data element keywords for DHIS2 detection (signal 2)
MALARIA_KEYWORDS = frozenset({
    'rdt', 'microscopy', 'tpr', 'malaria', 'tested', 'positive',
    'fever', 'artesunate', 'artemether', 'chloroquine', 'llin',
})


# --------------------------------------------------------------------------- #
#  Mode helper
# --------------------------------------------------------------------------- #

def get_cleaner_mode() -> str:
    """
    Parse CHATMRPT_DHIS2_CLEANER env var.
    Returns one of: 'off', 'log_only', 'full'
    """
    raw = os.getenv('CHATMRPT_DHIS2_CLEANER', 'off').strip().lower()
    if raw in ('true', '1', 'yes', 'full'):
        return 'full'
    if raw == 'log_only':
        return 'log_only'
    return 'off'


# --------------------------------------------------------------------------- #
#  Cleaning Report dataclass
# --------------------------------------------------------------------------- #

@dataclass
class CleaningReport:
    """Structured record of every cleaner decision for transparency."""
    cleaning_applied: bool = False
    detected_as: Optional[str] = None
    detection_signals: List[str] = field(default_factory=list)
    original_shape: Tuple[int, int] = (0, 0)
    cleaned_shape: Tuple[int, int] = (0, 0)
    mojibake_fixed: List[Dict[str, str]] = field(default_factory=list)
    duplicates_merged: List[Dict[str, Any]] = field(default_factory=list)
    data_quality_warnings: List[Dict[str, Any]] = field(default_factory=list)
    validation_checks: Dict[str, str] = field(default_factory=dict)
    column_rename_map: Dict[str, str] = field(default_factory=dict)
    fallback_reason: Optional[str] = None
    mode: str = 'full'

    def to_dict(self) -> dict:
        return {
            'cleaning_applied': self.cleaning_applied,
            'detected_as': self.detected_as,
            'detection_signals': self.detection_signals,
            'original_shape': list(self.original_shape),
            'cleaned_shape': list(self.cleaned_shape),
            'mojibake_fixed': self.mojibake_fixed,
            'duplicates_merged': self.duplicates_merged,
            'data_quality_warnings': self.data_quality_warnings,
            'validation_checks': self.validation_checks,
            'column_rename_map': self.column_rename_map,
            'fallback_reason': self.fallback_reason,
            'mode': self.mode,
        }


class CleaningIntegrityError(Exception):
    """Raised when a blocking validation check fails."""
    pass


# --------------------------------------------------------------------------- #
#  Detection
# --------------------------------------------------------------------------- #

def is_dhis2_export(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Detect if this DataFrame looks like a DHIS2/HMIS malaria surveillance export.

    Requires ALL THREE signals to be true:
      1. period: has 'periodname', 'periodcode', 'period0me' (mojibake), etc.
      2. malaria_multi: has AT LEAST TWO distinct malaria data element keywords
      3. facility_hierarchy: has 'orgunitlevel*' or 'organisationunit*' columns

    Returns (is_dhis2, list_of_detected_signals).
    """
    col_text = ' '.join(df.columns).lower()

    # Signal 1: period column
    has_period = any(
        'period' in c.lower() or c.lower() in ('year', 'month', 'date')
        for c in df.columns
    )

    # Signal 2: at least 2 distinct malaria keywords
    distinct_malaria = sum(1 for kw in MALARIA_KEYWORDS if kw in col_text)
    has_malaria_multi = distinct_malaria >= 2

    # Signal 3: facility hierarchy
    has_facility_hierarchy = any(
        'orgunitlevel' in c.lower() or 'organisationunit' in c.lower()
        for c in df.columns
    )

    signals = []
    if has_period:
        signals.append('period_column')
    if has_malaria_multi:
        signals.append(f'malaria_terminology ({distinct_malaria} keywords)')
    if has_facility_hierarchy:
        signals.append('facility_hierarchy')

    is_dhis2 = has_period and has_malaria_multi and has_facility_hierarchy
    return is_dhis2, signals


# --------------------------------------------------------------------------- #
#  Mojibake fix
# --------------------------------------------------------------------------- #

def fix_mojibake(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Apply whitelist of known mojibake patterns to column names.
    Returns (renamed_df, rename_map).
    Only emits rename entries where source != target.
    """
    rename_map: Dict[str, str] = {}
    for col in list(df.columns):
        new_col = col
        for pattern, replacement in MOJIBAKE_PATTERNS:
            new_col = pattern.sub(replacement, new_col)
        if new_col != col:
            rename_map[col] = new_col

    if not rename_map:
        return df, {}

    cleaned = df.rename(columns=rename_map)
    return cleaned, rename_map


# --------------------------------------------------------------------------- #
#  Period column detection
# --------------------------------------------------------------------------- #

def detect_period_column(df: pd.DataFrame) -> Optional[str]:
    """
    Heuristic-based period column detection.

    Priority:
      1. Exact name matches: 'period', 'periodname', 'periodcode', 'Period'
      2. Columns containing 'period' in the name
      3. Integer/numeric columns with values all in [2000, 2030]
    """
    # Priority 1: exact matches
    for name in ('period', 'periodname', 'periodcode', 'Period', 'PeriodName'):
        if name in df.columns:
            return name

    # Priority 2: contains 'period'
    for c in df.columns:
        if 'period' in c.lower():
            return c

    # Priority 3: integer columns with year-like values
    for c in df.columns:
        try:
            if pd.api.types.is_numeric_dtype(df[c]):
                vals = df[c].dropna()
                if len(vals) > 0 and vals.between(2000, 2030).all():
                    return c
        except Exception:
            continue

    return None


# --------------------------------------------------------------------------- #
#  Duplicate detection
# --------------------------------------------------------------------------- #

_DUP_PATTERN = re.compile(r'^(.+?)\.(\d+)$')


def detect_duplicate_groups(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Find pandas-auto-renamed duplicate columns.

    Pattern: when a source file has duplicate headers, pandas renames them as
    'col', 'col.1', 'col.2'. We only treat something as a duplicate if the
    base name (without .N) also exists — this prevents false positives on
    legitimate column names that happen to end in .N (e.g., 'version_1.0').

    Returns: {base_name: [base, base.1, base.2, ...]}
    """
    groups: Dict[str, List[str]] = {}
    for col in df.columns:
        m = _DUP_PATTERN.match(col)
        if m:
            base = m.group(1)
            if base in df.columns:
                if base not in groups:
                    groups[base] = [base]
                groups[base].append(col)
    return groups


# --------------------------------------------------------------------------- #
#  Group analysis (decision tree)
# --------------------------------------------------------------------------- #

_RATIO_KEYWORDS = ('rate', 'percent', '%', 'ratio', 'tpr', 'proportion')


def _is_ratio_column(col_name: str) -> bool:
    lowered = col_name.lower()
    return any(kw in lowered for kw in _RATIO_KEYWORDS)


def analyze_group(
    df: pd.DataFrame,
    cols: List[str],
    period_col: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Decide merge strategy for a group of duplicate columns.

    Returns (strategy, reason) where strategy is one of:
      'sum', 'combine_first', 'drop_extra'

    Rules checked in order (first match wins):
      Rule 1: non-numeric → drop_extra
      Rule 2: identical values → drop_extra  (checked BEFORE ratio)
      Rule 3: ratio/percentage → combine_first
      Rule 4a: period-level asymmetry → sum
      Rule 4b: row-level non-zero complementarity → sum
      Rule 5: default → sum (with warning)
    """
    # Rule 1: non-numeric
    if not all(pd.api.types.is_numeric_dtype(df[c]) for c in cols):
        return 'drop_extra', 'non-numeric duplicates, cannot safely merge'

    # Rule 2: identical values (only checks 2-column groups for simplicity)
    if len(cols) == 2:
        c1, c2 = cols
        both = df[[c1, c2]].dropna()
        if len(both) > 0 and (both[c1] == both[c2]).all():
            return 'drop_extra', 'columns contain identical values (exact copies)'

    # Rule 3: ratio columns (checked AFTER identical, per v4 reordering)
    if any(_is_ratio_column(c) for c in cols):
        return 'combine_first', 'ratio/percentage column (cannot sum)'

    # Rule 4a: period-level coverage asymmetry (PRIMARY signal)
    if period_col and period_col in df.columns:
        coverage: Dict[str, int] = {}
        try:
            unique_periods = df[period_col].dropna().unique()
            for c in cols:
                active = sum(
                    1 for p in unique_periods
                    if (df.loc[df[period_col] == p, c].fillna(0) > 0).any()
                )
                coverage[c] = active

            max_cov = max(coverage.values()) if coverage else 0
            min_cov = min(coverage.values()) if coverage else 0

            if max_cov >= 2 * max(min_cov, 1):
                return 'sum', (
                    f'period coverage asymmetry: {coverage}; '
                    f'likely DHIS2 form migration'
                )
        except Exception as exc:
            logger.debug(f"Period-level analysis failed: {exc}")

    # Rule 4b: row-level non-zero complementarity (FALLBACK)
    if len(cols) == 2:
        c1, c2 = cols
        both_nz = ((df[c1].fillna(0) > 0) & (df[c2].fillna(0) > 0)).sum()
        any_nz = ((df[c1].fillna(0) > 0) | (df[c2].fillna(0) > 0)).sum()
        non_overlap = 1 - (both_nz / max(any_nz, 1))
        if non_overlap >= 0.80:
            return 'sum', (
                f'row-level complementarity '
                f'({non_overlap:.0%} non-overlapping)'
            )

    # Rule 5: default (with warning)
    return 'sum', 'ambiguous relationship; summing as safer count default'


# --------------------------------------------------------------------------- #
#  Group merge
# --------------------------------------------------------------------------- #

def merge_group(
    df: pd.DataFrame,
    cols: List[str],
    strategy: str,
) -> pd.DataFrame:
    """
    Apply merge strategy to a group of duplicate columns.
    The base column (cols[0]) is preserved; the .N variants are dropped.
    """
    base = cols[0]
    extras = [c for c in cols if c != base]

    if strategy == 'sum':
        # fillna(0).sum() treats None as 0 for summation.
        # NaN is only preserved when ALL source columns are NaN for that row.
        merged = df[cols].fillna(0).sum(axis=1)
        all_nan_mask = df[cols].isna().all(axis=1)
        merged = merged.where(~all_nan_mask, np.nan)
        df[base] = merged

    elif strategy == 'combine_first':
        result = df[base]
        for c in extras:
            result = result.combine_first(df[c])
        df[base] = result

    # drop_extra: no value change; just drop extras below

    return df.drop(columns=extras)


# --------------------------------------------------------------------------- #
#  Ratio column conflict detection (warning helper)
# --------------------------------------------------------------------------- #

def _check_ratio_conflicts(df: pd.DataFrame, cols: List[str]) -> int:
    """
    For ratio columns about to be combined with combine_first, count rows
    where both columns have distinct non-null values (the suppressed data).
    Returns the number of conflicting rows.
    """
    if len(cols) != 2:
        return 0
    c1, c2 = cols
    both_set = df[c1].notna() & df[c2].notna()
    different = df[c1] != df[c2]
    return int((both_set & different).sum())


# --------------------------------------------------------------------------- #
#  Validation checks
# --------------------------------------------------------------------------- #

def validate_cleaning(
    df_original: pd.DataFrame,
    df_cleaned: pd.DataFrame,
    report: CleaningReport,
) -> None:
    """
    Run BLOCKING and WARNING checks. Raises CleaningIntegrityError on
    blocking failures. Adds warnings directly to the report.
    """
    # Check B1: Row count preserved
    if len(df_cleaned) != len(df_original):
        raise CleaningIntegrityError(
            f"Row count changed: {len(df_original)} → {len(df_cleaned)}"
        )
    report.validation_checks['row_count_preserved'] = 'pass'

    # Check B2: Column count non-increasing
    if len(df_cleaned.columns) > len(df_original.columns):
        raise CleaningIntegrityError(
            f"Column count increased: {len(df_original.columns)} → {len(df_cleaned.columns)}"
        )
    report.validation_checks['column_count_non_increasing'] = 'pass'

    # Check W1: Temporal consistency (IQR-based, informational)
    # Skipped for now — requires period column and merged count columns,
    # added as a future enhancement.
    report.validation_checks['temporal_consistency'] = 'skipped (future)'


# --------------------------------------------------------------------------- #
#  Schema coordination helper
# --------------------------------------------------------------------------- #

def apply_rename_map_to_schema(
    schema: Dict[str, Any],
    rename_map: Dict[str, str],
) -> Dict[str, Any]:
    """
    Update a schema dict so that any string values referencing column names
    are remapped through the cleaner's rename_map. Non-string values
    (e.g., header_row int) are passed through unchanged.
    """
    if not rename_map:
        return dict(schema)

    updated: Dict[str, Any] = {}
    for field_name, value in schema.items():
        if isinstance(value, str) and value in rename_map:
            updated[field_name] = rename_map[value]
        else:
            updated[field_name] = value
    return updated


# --------------------------------------------------------------------------- #
#  Raw file selector
# --------------------------------------------------------------------------- #

def _select_raw_upload_file(data_files: List[str]) -> str:
    """
    Select the original user-uploaded file from a list of candidates.

    Preference order:
      1. Most recently created XLS/XLSX file (always a user upload in our pipeline)
      2. Most recently created CSV that isn't in INTERMEDIATE_FILES
      3. Raises FileNotFoundError if neither is available
    """
    xls_candidates = [
        f for f in data_files
        if f.lower().endswith(('.xls', '.xlsx'))
    ]
    if xls_candidates:
        return max(xls_candidates, key=os.path.getctime)

    csv_candidates = [
        f for f in data_files
        if f.lower().endswith('.csv')
        and os.path.basename(f) not in INTERMEDIATE_FILES
    ]
    if csv_candidates:
        return max(csv_candidates, key=os.path.getctime)

    raise FileNotFoundError(
        "No raw upload file found (all candidates are intermediate files)"
    )


# --------------------------------------------------------------------------- #
#  Main entry point
# --------------------------------------------------------------------------- #

def clean_dhis2_export(
    df: pd.DataFrame,
    mode: str = 'full',
) -> Tuple[pd.DataFrame, CleaningReport]:
    """
    Detect DHIS2 exports and clean duplicate columns + mojibake.

    Args:
        df: Input DataFrame (raw, possibly with duplicates and mojibake)
        mode: 'full' (apply changes) or 'log_only' (compute but discard)

    Returns:
        (cleaned_df, report) where cleaned_df may be the original if:
        - detection returns False (not a DHIS2 file)
        - mode is 'log_only'
        - a blocking validation check failed (fall-back)
        - an exception was caught internally (defensive)
    """
    report = CleaningReport(
        original_shape=df.shape,
        cleaned_shape=df.shape,
        mode=mode,
    )

    # STEP 1: Detection
    is_dhis2, signals = is_dhis2_export(df)
    report.detection_signals = signals
    if not is_dhis2:
        report.cleaning_applied = False
        return df, report

    report.detected_as = "DHIS2 malaria export"

    try:
        # STEP 2: Mojibake fix FIRST (so duplicate detection sees clean names)
        cleaned, mojibake_renames = fix_mojibake(df.copy())
        report.mojibake_fixed = [
            {"from": k, "to": v} for k, v in mojibake_renames.items()
        ]
        report.column_rename_map.update(mojibake_renames)

        # STEP 3: Period column detection (for Rule 4a)
        period_col = detect_period_column(cleaned)

        # STEP 4: Duplicate group detection
        groups = detect_duplicate_groups(cleaned)

        # STEP 5: Analyze and merge each group
        for base, cols in groups.items():
            strategy, reason = analyze_group(cleaned, cols, period_col=period_col)

            # Ratio conflict warning (only for combine_first)
            if strategy == 'combine_first':
                n_conflicts = _check_ratio_conflicts(cleaned, cols)
                if n_conflicts > 0:
                    report.data_quality_warnings.append({
                        'type': 'ratio_column_conflict',
                        'base_column': base,
                        'conflicting_rows': n_conflicts,
                        'description': (
                            f'{n_conflicts} rows had distinct non-null values in both '
                            f'ratio columns; combine_first kept the first, discarded the second'
                        ),
                    })

            # Rule 5 warning (ambiguous sum)
            if 'ambiguous' in reason:
                report.data_quality_warnings.append({
                    'type': 'ambiguous_duplicate_merge',
                    'base_column': base,
                    'strategy': strategy,
                    'description': (
                        f'Duplicate group had no clear complementarity signal; '
                        f'defaulted to {strategy}. Review if results look double-counted.'
                    ),
                })

            # Compute source totals for the report
            source_totals = {}
            for c in cols:
                try:
                    if pd.api.types.is_numeric_dtype(cleaned[c]):
                        source_totals[c] = float(cleaned[c].fillna(0).sum())
                except Exception:
                    source_totals[c] = None

            cleaned = merge_group(cleaned, cols, strategy)

            # Compute merged total
            merged_total = None
            try:
                if base in cleaned.columns and pd.api.types.is_numeric_dtype(cleaned[base]):
                    merged_total = float(cleaned[base].fillna(0).sum())
            except Exception:
                pass

            report.duplicates_merged.append({
                'base_column': base,
                'n_source_columns': len(cols),
                'source_columns': cols,
                'strategy': strategy,
                'reason': reason,
                'source_totals': source_totals,
                'merged_total': merged_total,
            })

        # STEP 6: Validation
        validate_cleaning(df, cleaned, report)

        report.cleaned_shape = cleaned.shape

        # STEP 7: Log-only mode — compute report but return original
        if mode == 'log_only':
            report.cleaning_applied = True  # detection fired
            # Note: we still return df (original), not cleaned
            return df, report

        report.cleaning_applied = True
        return cleaned, report

    except CleaningIntegrityError as exc:
        logger.error(f"[DHIS2_CLEANER] Integrity check failed: {exc}")
        report.cleaning_applied = False
        report.fallback_reason = f"Integrity check failed: {exc}"
        return df, report
    except Exception as exc:
        logger.exception(f"[DHIS2_CLEANER] Unexpected error: {exc}")
        report.cleaning_applied = False
        report.fallback_reason = f"Unexpected error: {exc}"
        return df, report
