"""Reusable analytics helpers for Data Analysis V3."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional

import pandas as pd

# Column constants (note: strings reflect current CSV headers)
RDT_TEST_COLS = [
    "Persons presenting with fever & tested by RDT <5yrs",
    "Persons presenting with fever & tested by RDT  â‰¥5yrs (excl PW)",
    "Persons presenting with fever & tested by RDT Preg Women (PW)",
]

RDT_POSITIVE_COLS = [
    "Persons tested positive for malaria by RDT <5yrs",
    "Persons tested positive for malaria by RDT  â‰¥5yrs (excl PW)",
    "Persons tested positive for malaria by RDT Preg Women (PW)",
]

MIC_TEST_COLS = [
    "Persons presenting with fever and tested by Microscopy <5yrs",
    "Persons presenting with fever and tested by Microscopy  â‰¥5yrs (excl PW)",
    "Persons presenting with fever and tested by Microscopy Preg Women (PW)",
]

MIC_POSITIVE_COLS = [
    "Persons tested positive for malaria by Microscopy <5yrs",
    "Persons tested positive for malaria by Microscopy  â‰¥5yrs (excl PW)",
    "Persons tested positive for malaria by Microscopy Preg Women (PW)",
]

PERIOD_COLUMN = "periodcode"


def _resolve_columns(df: pd.DataFrame, columns: Iterable[str]) -> List[str]:
    resolved: List[str] = []
    for col in columns:
        if col in df.columns:
            resolved.append(col)
            continue
        # Try to normalise the "â‰¥" artefact to a proper >= symbol
        candidate = col.replace("â‰¥", "≥")
        if candidate in df.columns:
            resolved.append(candidate)
            continue
        candidate = col.replace("â‰¥", ">=")
        if candidate in df.columns:
            resolved.append(candidate)
            continue
    return resolved


def _safe_sum_df(df: pd.DataFrame, columns: Iterable[str]) -> pd.Series:
    existing = _resolve_columns(df, columns)
    if not existing:
        return pd.Series(0, index=df.index)
    return df[existing].fillna(0).sum(axis=1)


def _safe_sum_total(df: pd.DataFrame, columns: Iterable[str]) -> float:
    existing = _resolve_columns(df, columns)
    if not existing:
        return 0.0
    return float(df[existing].fillna(0).to_numpy().sum())


def summarize_rdt_usage(df: pd.DataFrame, group_by: Optional[List[str]] = None) -> pd.DataFrame:
    """Aggregate RDT testing / positivity metrics."""
    if group_by:
        grouped = df.groupby(group_by, dropna=False)
        summaries = grouped.apply(_summarize_rdt_core).reset_index()
        return summaries
    return _summarize_rdt_core(df).to_frame().T


def _summarize_rdt_core(df: pd.DataFrame) -> pd.Series:
    tests = _safe_sum_total(df, RDT_TEST_COLS)
    positives = _safe_sum_total(df, RDT_POSITIVE_COLS)
    positivity = float(positives / tests) if tests else None
    return pd.Series({
        "rdt_tests": tests,
        "rdt_positives": positives,
        "rdt_positivity": positivity,
    })


def llin_distribution_over_time(df: pd.DataFrame) -> pd.DataFrame:
    """Return LLIN totals per period (pregnant women and under-5s) with missing share."""
    if PERIOD_COLUMN not in df.columns:
        raise ValueError("periodcode column is required for LLIN trend analysis")
    value_cols = [col for col in ["PW who received LLIN", "Children <5 yrs who received LLIN"] if col in df.columns]
    if not value_cols:
        raise ValueError("LLIN columns not found")
    totals = df.groupby(PERIOD_COLUMN, dropna=False)[value_cols].sum().sort_index()
    if "PW who received LLIN" in df.columns:
        totals["pw_missing_share"] = df.groupby(PERIOD_COLUMN)["PW who received LLIN"].apply(lambda s: float(s.isna().mean()))
    return totals.reset_index().rename(columns={PERIOD_COLUMN: "periodcode"})


def top_facilities_last_quarter(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Facilities with highest adult RDT positivity in the latest quarter."""
    if PERIOD_COLUMN not in df.columns:
        raise ValueError("periodcode column is required for quarterly ranking")
    codes = sorted(df[PERIOD_COLUMN].dropna().unique())
    if not codes:
        raise ValueError("No periodcode values present")
    latest_codes = codes[-3:]
    filtered = df[df[PERIOD_COLUMN].isin(latest_codes)]
    grouped = filtered.groupby("HealthFacility", dropna=False)
    tests = grouped.apply(lambda g: _safe_sum_total(g, [RDT_TEST_COLS[1]])).rename("tests_adult")
    positives = grouped.apply(lambda g: _safe_sum_total(g, [RDT_POSITIVE_COLS[1]])).rename("positives_adult")
    result = pd.concat([tests, positives], axis=1)
    result["positivity_adult"] = result.apply(lambda row: float(row["positives_adult"]/row["tests_adult"]) if row["tests_adult"] else None, axis=1)
    result = result.sort_values(["positivity_adult", "tests_adult"], ascending=[False, False]).head(n)
    return result.reset_index().rename(columns={"HealthFacility": "facility"})


def low_testing_high_positivity_wards(
    df: pd.DataFrame,
    tests_quantile: float = 0.25,
    positivity_threshold: float = 0.8,
    n: int = 10,
) -> pd.DataFrame:
    """Return wards with tests below quantile but positivity above threshold."""
    grouped = df.groupby(['LGA', 'WardName'], dropna=False)
    tests = grouped.apply(lambda g: _safe_sum_total(g, RDT_TEST_COLS)).rename("tests_total")
    positives = grouped.apply(lambda g: _safe_sum_total(g, RDT_POSITIVE_COLS)).rename("positives_total")
    stats = pd.concat([tests, positives], axis=1)
    stats["positivity"] = stats.apply(lambda row: float(row["positives_total"]/row["tests_total"]) if row["tests_total"] else None, axis=1)
    low_threshold = stats["tests_total"].quantile(tests_quantile)
    filtered = stats[(stats["tests_total"] < low_threshold) & (stats["positivity"] > positivity_threshold)]
    return filtered.sort_values("positivity", ascending=False).head(n).reset_index()


def microscopy_usage_by_facility(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Top facilities by adult microscopy testing volume."""
    grouped = df.groupby(['LGA', 'WardName', 'HealthFacility'], dropna=False)
    adult_mic = grouped.apply(lambda g: _safe_sum_total(g, [MIC_TEST_COLS[1]])).rename("adult_microscopy_tests")
    return adult_mic.sort_values(ascending=False).head(n).reset_index()


def facility_level_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregated testing distribution per facility level."""
    if 'FacilityLevel' not in df.columns:
        raise ValueError('FacilityLevel column missing')
    grouped = df.groupby('FacilityLevel', dropna=False)
    tests_total = grouped.apply(lambda g: _safe_sum_total(g, RDT_TEST_COLS)).rename("tests_total")
    positives_total = grouped.apply(lambda g: _safe_sum_total(g, RDT_POSITIVE_COLS)).rename("positives_total")
    under5 = grouped.apply(lambda g: _safe_sum_total(g, [RDT_TEST_COLS[0]])).rename("under5_tests")
    adult = grouped.apply(lambda g: _safe_sum_total(g, [RDT_TEST_COLS[1]])).rename("adult_tests")
    summary = pd.concat([tests_total, positives_total, under5, adult], axis=1)
    summary["positivity"] = summary.apply(lambda row: float(row["positives_total"]/row["tests_total"]) if row["tests_total"] else None, axis=1)
    summary["under5_share"] = summary.apply(lambda row: float(row["under5_tests"]/(row["under5_tests"]+row["adult_tests"])) if (row["under5_tests"]+row["adult_tests"]) else None, axis=1)
    summary["adult_share"] = summary.apply(lambda row: float(row["adult_tests"]/(row["under5_tests"]+row["adult_tests"])) if (row["under5_tests"]+row["adult_tests"]) else None, axis=1)
    return summary.reset_index().rename(columns={'FacilityLevel': 'facility_level'}).sort_values('tests_total', ascending=False)


def dataset_health_report(df: pd.DataFrame, missing_threshold: float = 0.5) -> Dict[str, Any]:
    """Return basic shape/missingness diagnostics for a dataframe."""
    if not isinstance(df, pd.DataFrame):
        return {}

    report: Dict[str, Any] = {
        'rows': int(df.shape[0]),
        'columns': int(df.shape[1]),
    }

    if df.shape[0] == 0:
        report['status'] = 'empty'
        return report

    missing_ratio = df.isna().mean()
    high_missing = missing_ratio[missing_ratio > missing_threshold].sort_values(ascending=False)
    if not high_missing.empty:
        report['high_missing_columns'] = [
            {'column': col, 'missing_ratio': round(float(ratio), 3)}
            for col, ratio in high_missing.items()
        ]
    return report
