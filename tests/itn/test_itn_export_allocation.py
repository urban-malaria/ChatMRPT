"""The ITN export lists nets for every ward, including wards that received none."""
import pandas as pd

from app.planning.export_tools import ALLOCATION_COLUMNS, build_ward_allocation


def _plan():
    unified = pd.DataFrame({
        'WardName': ['Alpha', 'Beta ', 'gamma', 'Delta'],
        'LGA': ['L1', 'L1', 'L2', 'L2'],
        'composite_rank': [1, 2, 3, 4],
    })
    itn_results = {
        'all_wards': [
            {'WardName': 'Alpha', 'Population': 1000, 'has_population_data': True, 'nets_needed': 556},
            {'WardName': 'Beta', 'Population': 2000, 'has_population_data': True, 'nets_needed': 1112},
            {'WardName': 'Gamma', 'Population': 1500.4, 'has_population_data': False, 'nets_needed': 834},
            {'WardName': 'Delta', 'Population': 800, 'has_population_data': True, 'nets_needed': 445},
        ],
        'prioritized': [
            {'WardName': 'Alpha', 'Population': 1000, 'nets_needed': 556, 'nets_allocated': 556,
             'coverage_percent': 100.0, 'allocation_phase': 'Rural Priority (Full)'},
            {'WardName': 'Beta', 'Population': 2000, 'nets_needed': 1112, 'nets_allocated': 444,
             'coverage_percent': 39.9, 'allocation_phase': 'Rural Priority (Partial)'},
        ],
        'reprioritized': [],
    }
    return unified, itn_results


def test_every_ward_has_allocation_columns():
    unified, itn_results = _plan()
    table = build_ward_allocation(unified, itn_results)
    assert list(table.index) == list(unified.index)
    assert set(ALLOCATION_COLUMNS) <= set(table.columns)
    assert table['nets_allocated'].tolist() == [556, 444, 0, 0]
    assert table['nets_allocated'].sum() == 1000
    assert table['allocation_phase'].tolist()[2:] == ['Not allocated', 'Not allocated']
    assert table['population'].tolist() == [1000, 2000, 1500, 800]
    assert table['population_is_estimate'].tolist() == [False, False, True, False]


def test_plan_without_all_wards_uses_allocated_wards():
    unified, itn_results = _plan()
    itn_results.pop('all_wards')
    table = build_ward_allocation(unified, itn_results)
    assert table['nets_allocated'].tolist() == [556, 444, 0, 0]
    assert table['population'].iloc[0] == 1000
    assert pd.isna(table['population'].iloc[3])


def test_no_plan_gives_zero_nets():
    unified, _ = _plan()
    table = build_ward_allocation(unified, {})
    assert table['nets_allocated'].tolist() == [0, 0, 0, 0]
    assert (table['allocation_phase'] == 'Not allocated').all()
