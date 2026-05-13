import pandas as pd

from app.analysis.itn_pipeline import _format_rank_range, _risk_tier_from_rank


def test_risk_tier_from_rank_splits_ranked_wards_into_thirds():
    assert _risk_tier_from_rank(1, 9) == 'High Risk'
    assert _risk_tier_from_rank(3, 9) == 'High Risk'
    assert _risk_tier_from_rank(4, 9) == 'Medium Risk'
    assert _risk_tier_from_rank(6, 9) == 'Medium Risk'
    assert _risk_tier_from_rank(7, 9) == 'Low Risk'


def test_risk_tier_from_rank_handles_missing_values():
    assert _risk_tier_from_rank(pd.NA, 9) == 'Unranked'
    assert _risk_tier_from_rank(1, 0) == 'Unranked'


def test_format_rank_range_for_allocated_tier():
    frame = pd.DataFrame({'overall_rank': [20, 4, None, 10]})
    assert _format_rank_range(frame) == '4-20'
