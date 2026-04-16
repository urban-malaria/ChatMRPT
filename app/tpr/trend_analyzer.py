"""
Pre-computes per-ward trend statistics from the multi-year TPR time series.

Produces trend_summary.csv for fast lookup of common questions (slope, direction,
year-over-year delta). Open-ended trend queries are handled dynamically by the
agent's analyze_data tool against tpr_time_series.csv.
"""

import numpy as np
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)

WORSENING_SLOPE_THRESHOLD = 5.0    # burden increase per year → worsening
IMPROVING_SLOPE_THRESHOLD = -5.0   # burden decrease per year → improving
HOTSPOT_THRESHOLD = 200.0          # burden per 1,000 — high risk cutoff


def _linear_slope(values: pd.Series) -> Optional[float]:
    """OLS slope of values against their integer index. Returns None if < 2 points."""
    v = values.dropna()
    if len(v) < 2:
        return None
    x = np.arange(len(v), dtype=float)
    x -= x.mean()
    y = v.values.astype(float)
    y -= y.mean()
    denom = (x * x).sum()
    return float((x * y).sum() / denom) if denom != 0 else 0.0


def compute_trend(ts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-ward trend statistics from a multi-year time series DataFrame.

    Args:
        ts_df: Must have columns WardName, LGA, Period, Burden.
               Wards with fewer than 2 years of Burden data are excluded.
    Returns:
        DataFrame with one row per ward:
            WardName, LGA, Slope, Direction, Delta_Latest,
            Burden_First, Burden_Latest, Years_Count
        Empty DataFrame if ts_df has < 2 distinct periods or no Burden column.
    """
    required = {'WardName', 'Period', 'Burden'}
    if not required.issubset(ts_df.columns):
        logger.warning(f"compute_trend: missing columns {required - set(ts_df.columns)}")
        return pd.DataFrame()

    periods = sorted(ts_df['Period'].dropna().unique())
    if len(periods) < 2:
        logger.info("compute_trend: < 2 periods — skipping trend computation")
        return pd.DataFrame()

    rows = []
    lga_col = 'LGA' if 'LGA' in ts_df.columns else None

    for ward, grp in ts_df.groupby('WardName'):
        ward_series = (
            grp.sort_values('Period')
               .drop_duplicates('Period')
               .set_index('Period')['Burden']
               .reindex(periods)
        )
        valid_count = int(ward_series.notna().sum())
        if valid_count < 2:
            continue

        slope = _linear_slope(ward_series)
        if slope is None:
            continue

        if slope >= WORSENING_SLOPE_THRESHOLD:
            direction = 'worsening'
        elif slope <= IMPROVING_SLOPE_THRESHOLD:
            direction = 'improving'
        else:
            direction = 'stable'

        first_valid = ward_series.dropna().iloc[0]
        latest_valid = ward_series.dropna().iloc[-1]

        # Delta between last two available periods
        available = ward_series.dropna()
        delta_latest = float(available.iloc[-1] - available.iloc[-2]) if len(available) >= 2 else None

        row = {
            'WardName': ward,
            'Slope': round(slope, 3),
            'Direction': direction,
            'Delta_Latest': round(delta_latest, 1) if delta_latest is not None else None,
            'Burden_First': round(float(first_valid), 1),
            'Burden_Latest': round(float(latest_valid), 1),
            'Years_Count': valid_count,
        }
        if lga_col:
            lga_val = grp['LGA'].dropna().iloc[0] if not grp['LGA'].dropna().empty else None
            row['LGA'] = lga_val
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    col_order = ['WardName']
    if lga_col:
        col_order.append('LGA')
    col_order += ['Slope', 'Direction', 'Delta_Latest', 'Burden_First', 'Burden_Latest', 'Years_Count']
    result = result[[c for c in col_order if c in result.columns]]
    logger.info(f"compute_trend: {len(result)} wards with trend data")
    return result


def identify_emerging_hotspots(ts_df: pd.DataFrame,
                                threshold: float = HOTSPOT_THRESHOLD) -> pd.DataFrame:
    """
    Wards that were below threshold in the earliest year and above in the latest.

    Args:
        ts_df: WardName, Period, Burden columns required.
        threshold: Burden per 1,000 cutoff.
    Returns:
        DataFrame of matching wards with WardName, Burden_First, Burden_Latest.
    """
    if ts_df.empty or 'Burden' not in ts_df.columns:
        return pd.DataFrame()

    periods = sorted(ts_df['Period'].dropna().unique())
    if len(periods) < 2:
        return pd.DataFrame()

    first_period, last_period = periods[0], periods[-1]
    first = ts_df[ts_df['Period'] == first_period].set_index('WardName')['Burden']
    last = ts_df[ts_df['Period'] == last_period].set_index('WardName')['Burden']

    common = first.index.intersection(last.index)
    emerging = common[(first[common] < threshold) & (last[common] >= threshold)]

    return pd.DataFrame({
        'WardName': emerging,
        'Burden_First': first[emerging].values,
        'Burden_Latest': last[emerging].values,
    })


def identify_resolving_hotspots(ts_df: pd.DataFrame,
                                 threshold: float = HOTSPOT_THRESHOLD) -> pd.DataFrame:
    """
    Wards that were above threshold in the earliest year and below in the latest.
    """
    if ts_df.empty or 'Burden' not in ts_df.columns:
        return pd.DataFrame()

    periods = sorted(ts_df['Period'].dropna().unique())
    if len(periods) < 2:
        return pd.DataFrame()

    first_period, last_period = periods[0], periods[-1]
    first = ts_df[ts_df['Period'] == first_period].set_index('WardName')['Burden']
    last = ts_df[ts_df['Period'] == last_period].set_index('WardName')['Burden']

    common = first.index.intersection(last.index)
    resolving = common[(first[common] >= threshold) & (last[common] < threshold)]

    return pd.DataFrame({
        'WardName': resolving,
        'Burden_First': first[resolving].values,
        'Burden_Latest': last[resolving].values,
    })
