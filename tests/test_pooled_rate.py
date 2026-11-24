"""Positivity across records is total positives / total tested, not the mean of row rates."""
import math

import pandas as pd

from app.agent.executor_simple import _inject_helpers


def _pooled_rate():
    exec_globals = {}
    _inject_helpers(exec_globals)
    return exec_globals['pooled_rate']


DATA = pd.DataFrame({
    'lga': ['A', 'A', 'B', 'B'],
    'positive': [90, 1, 0, 50],
    'tested': [100, 10, 0, 40],   # B has a record with no tests and one with positives > tests
})


def test_overall_rate_pools_counts():
    assert math.isclose(_pooled_rate()(DATA, 'positive', 'tested'), 91 / 110 * 100)


def test_rate_by_group_drops_invalid_records():
    table = _pooled_rate()(DATA, 'positive', 'tested', by='lga')
    assert math.isclose(table.loc['A', 'rate'], 91 / 110 * 100)
    assert 'B' not in table.index
