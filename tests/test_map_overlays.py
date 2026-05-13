import pandas as pd
import pytest

from app.utils.map_overlays import calculate_lga_averages


def test_calculate_lga_averages_uses_burden_multiplier_and_cap():
    df = pd.DataFrame({
        'LGACode': ['L1', 'L1', 'L2'],
        'Burden': [0, 0, 0],
        'Total_Positive': [1500, 100, 20],
        'Population': [1000, 1000, 1000],
    })

    result = calculate_lga_averages(
        df,
        'Burden',
        numerator_col='Total_Positive',
        denominator_col='Population',
        cap_value=1000,
    )

    assert result['L1'] == pytest.approx(800.0)
    assert result['L2'] == pytest.approx(20.0)


def test_calculate_lga_averages_respects_explicit_cap():
    df = pd.DataFrame({
        'LGACode': ['L1'],
        'Burden': [0],
        'Total_Positive': [2000],
        'Population': [1000],
    })

    result = calculate_lga_averages(
        df,
        'Burden',
        numerator_col='Total_Positive',
        denominator_col='Population',
        rate_multiplier=1000,
        cap_value=1000,
    )

    assert result['L1'] == pytest.approx(1000.0)


def test_calculate_lga_averages_caps_simple_mean_fallback():
    df = pd.DataFrame({
        'LGACode': ['L1', 'L1'],
        'Burden': [900, 1200],
    })

    result = calculate_lga_averages(df, 'Burden', cap_value=1000)

    assert result['L1'] == pytest.approx(1000.0)
