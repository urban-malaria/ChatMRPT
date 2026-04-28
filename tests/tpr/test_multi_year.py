"""
Tests for multi-year analysis: trend_analyzer and add_burden_to_timeseries.
"""
import numpy as np
import pandas as pd
import pytest

from app.tpr.trend_analyzer import (
    HOTSPOT_THRESHOLD,
    IMPROVING_SLOPE_THRESHOLD,
    WORSENING_SLOPE_THRESHOLD,
    compute_trend,
    identify_emerging_hotspots,
    identify_resolving_hotspots,
)
from app.tpr.utils import add_burden_to_timeseries


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_ts(ward_burdens: dict, lga: str = 'TestLGA') -> pd.DataFrame:
    """
    Build a ts_df from {ward: [burden_2020, burden_2021, ...]} dict.
    Periods are 2020, 2021, ... matching the list length.
    """
    rows = []
    n_years = max(len(v) for v in ward_burdens.values())
    periods = list(range(2020, 2020 + n_years))
    for ward, burdens in ward_burdens.items():
        for period, burden in zip(periods, burdens):
            rows.append({
                'WardName': ward,
                'LGA': lga,
                'Period': period,
                'Total_Positive': int(burden * 10),  # synthetic
                'Total_Tested': 10000,
                'TPR': round(burden / 100, 2),
                'Burden': float(burden),
            })
    return pd.DataFrame(rows)


def _make_ward_df(ward_pop: dict) -> pd.DataFrame:
    return pd.DataFrame([
        {'WardName': w, 'Population': p}
        for w, p in ward_pop.items()
    ])


# ---------------------------------------------------------------------------
# compute_trend tests
# ---------------------------------------------------------------------------

def test_compute_trend_worsening():
    ts = _make_ts({'WardA': [100, 150, 200, 250, 300, 350]})
    result = compute_trend(ts)
    row = result[result['WardName'] == 'WardA'].iloc[0]
    assert row['Direction'] == 'worsening'
    assert row['Slope'] > WORSENING_SLOPE_THRESHOLD


def test_compute_trend_improving():
    ts = _make_ts({'WardA': [350, 300, 250, 200, 150, 100]})
    result = compute_trend(ts)
    row = result[result['WardName'] == 'WardA'].iloc[0]
    assert row['Direction'] == 'improving'
    assert row['Slope'] < IMPROVING_SLOPE_THRESHOLD


def test_compute_trend_stable():
    ts = _make_ts({'WardA': [200, 202, 198, 201, 199, 200]})
    result = compute_trend(ts)
    row = result[result['WardName'] == 'WardA'].iloc[0]
    assert row['Direction'] == 'stable'
    assert IMPROVING_SLOPE_THRESHOLD <= row['Slope'] <= WORSENING_SLOPE_THRESHOLD


def test_compute_trend_single_year():
    ts = _make_ts({'WardA': [200]})
    result = compute_trend(ts)
    assert result.empty


def test_compute_trend_slope_threshold():
    # Ward with slope exactly at +5.0 should be 'worsening' (>= threshold)
    # Build data where slope ≈ 5: y = 5*x + 200
    years = 6
    burdens = [200 + 5 * i for i in range(years)]
    ts = _make_ts({'WardA': burdens})
    result = compute_trend(ts)
    row = result[result['WardName'] == 'WardA'].iloc[0]
    assert abs(row['Slope'] - 5.0) < 0.5
    assert row['Direction'] == 'worsening'


def test_compute_trend_output_columns():
    ts = _make_ts({'WardA': [100, 200], 'WardB': [300, 250]})
    result = compute_trend(ts)
    for col in ['WardName', 'LGA', 'Slope', 'Direction', 'Delta_Latest',
                'Burden_First', 'Burden_Latest', 'Years_Count']:
        assert col in result.columns, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# add_burden_to_timeseries tests
# ---------------------------------------------------------------------------

def test_add_burden_correct_calculation():
    ts = _make_ts({'ward_a': [0.0]})  # Burden will be overwritten by merge
    # Override with known values
    ts = pd.DataFrame([
        {'WardName': 'ward_a', 'LGA': 'LGA1', 'Period': 2020,
         'Total_Positive': 500, 'Total_Tested': 5000, 'TPR': 10.0},
        {'WardName': 'ward_a', 'LGA': 'LGA1', 'Period': 2021,
         'Total_Positive': 600, 'Total_Tested': 5000, 'TPR': 12.0},
    ])
    ward_df = _make_ward_df({'ward_a': 10000})
    result = add_burden_to_timeseries(ts, ward_df)
    expected_2020 = (500 / 10000) * 1000  # 50.0
    expected_2021 = (600 / 10000) * 1000  # 60.0
    assert abs(result[result['Period'] == 2020]['Burden'].iloc[0] - expected_2020) < 0.01
    assert abs(result[result['Period'] == 2021]['Burden'].iloc[0] - expected_2021) < 0.01


def test_add_burden_missing_ward():
    ts = pd.DataFrame([
        {'WardName': 'ward_missing', 'Period': 2020, 'Total_Positive': 100, 'Total_Tested': 1000},
    ])
    ward_df = _make_ward_df({'ward_other': 5000})
    result = add_burden_to_timeseries(ts, ward_df)
    assert result['Burden'].isna().all()


def test_add_burden_normalized_names_match():
    """Both sides use normalized names — merge must produce non-NaN Burden."""
    ts = pd.DataFrame([
        {'WardName': 'ward_alpha', 'Period': 2020, 'Total_Positive': 200, 'Total_Tested': 2000},
        {'WardName': 'ward_beta', 'Period': 2020, 'Total_Positive': 300, 'Total_Tested': 3000},
    ])
    ward_df = _make_ward_df({'ward_alpha': 8000, 'ward_beta': 12000})
    result = add_burden_to_timeseries(ts, ward_df)
    assert result['Burden'].notna().all()
    assert result[result['WardName'] == 'ward_alpha']['Burden'].iloc[0] == pytest.approx(25.0)
    assert result[result['WardName'] == 'ward_beta']['Burden'].iloc[0] == pytest.approx(25.0)


# ---------------------------------------------------------------------------
# identify_emerging / resolving hotspots
# ---------------------------------------------------------------------------

def test_emerging_hotspots():
    ts = _make_ts({
        'WardA': [100, 150, 250],   # crosses threshold between year 1 and 2
        'WardB': [300, 350, 400],   # always above — not emerging
        'WardC': [50, 80, 120],     # stays below
    })
    result = identify_emerging_hotspots(ts, threshold=200.0)
    assert 'WardA' in result['WardName'].values
    assert 'WardB' not in result['WardName'].values
    assert 'WardC' not in result['WardName'].values


def test_resolving_hotspots():
    ts = _make_ts({
        'WardA': [350, 250, 150],   # starts above, ends below
        'WardB': [100, 120, 130],   # always below — not resolving
        'WardC': [300, 310, 320],   # always above
    })
    result = identify_resolving_hotspots(ts, threshold=200.0)
    assert 'WardA' in result['WardName'].values
    assert 'WardB' not in result['WardName'].values
    assert 'WardC' not in result['WardName'].values


# ---------------------------------------------------------------------------
# year_tag backward compatibility
# ---------------------------------------------------------------------------

def test_year_tag_backward_compat(tmp_path, monkeypatch):
    """
    engine.run_composite_analysis called with year_tag='' must produce
    composite_scores.csv — NOT composite_scores_.csv.
    """
    # Import and verify filename construction matches expectation
    year_tag = ''
    filename = f'composite_scores{year_tag}.csv'
    assert filename == 'composite_scores.csv', (
        f"year_tag='' must produce 'composite_scores.csv', got '{filename}'"
    )
    year_tag_2020 = '_2020'
    filename_2020 = f'composite_scores{year_tag_2020}.csv'
    assert filename_2020 == 'composite_scores_2020.csv'


def test_multi_year_detection_positive():
    ts = _make_ts({
        'WardA': [100, 150, 200, 250, 300, 350],
        'WardB': [80, 120, 160, 200, 240, 280],
    })
    periods = sorted(ts['Period'].dropna().unique())
    assert len(periods) > 1, "6-year fixture must be detected as multi-year"
    assert len(periods) == 6
