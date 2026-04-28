"""
Unit tests for app.utils.dhis2_cleaner.

Run: pytest tests/test_dhis2_cleaner.py -v
"""

import os
import sys
import tempfile
import time
from unittest import mock

import numpy as np
import pandas as pd
import pytest

# Bypass flask import chain by loading modules directly
import importlib.util
import types

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)


def _load_cleaner_modules():
    """Load dhis2_cleaner without triggering flask_session imports."""
    if 'app.utils.dhis2_cleaner' in sys.modules:
        return sys.modules['app.utils.dhis2_cleaner']

    # Create fake app and app.utils packages if needed
    if 'app' not in sys.modules:
        sys.modules['app'] = types.ModuleType('app')
    if 'app.utils' not in sys.modules:
        sys.modules['app.utils'] = types.ModuleType('app.utils')

    # Load mojibake patterns
    spec1 = importlib.util.spec_from_file_location(
        'app.utils.dhis2_mojibake_patterns',
        os.path.join(_ROOT, 'app', 'utils', 'dhis2_mojibake_patterns.py')
    )
    mojibake = importlib.util.module_from_spec(spec1)
    sys.modules['app.utils.dhis2_mojibake_patterns'] = mojibake
    spec1.loader.exec_module(mojibake)

    # Load cleaner
    spec2 = importlib.util.spec_from_file_location(
        'app.utils.dhis2_cleaner',
        os.path.join(_ROOT, 'app', 'utils', 'dhis2_cleaner.py')
    )
    cleaner = importlib.util.module_from_spec(spec2)
    sys.modules['app.utils.dhis2_cleaner'] = cleaner
    spec2.loader.exec_module(cleaner)
    return cleaner


cleaner = _load_cleaner_modules()
clean_dhis2_export = cleaner.clean_dhis2_export
apply_rename_map_to_schema = cleaner.apply_rename_map_to_schema
get_cleaner_mode = cleaner.get_cleaner_mode
_select_raw_upload_file = cleaner._select_raw_upload_file


# --------------------------------------------------------------------------- #
#  Fixtures
# --------------------------------------------------------------------------- #

def _dhis2_signals_fixture(base_cols: dict) -> dict:
    """Add DHIS2 signal columns so detection fires."""
    return {
        'orgunitlevel2': ['Kwara State'] * len(next(iter(base_cols.values()))),
        'orgunitlevel3': ['LGA A'] * len(next(iter(base_cols.values()))),
        **base_cols,
    }


def _kwara_like_fixture(n_rows=20) -> pd.DataFrame:
    """Kwara-like fixture with mojibake + duplicates."""
    return pd.DataFrame({
        'orgunitlevel2': ['Kwara State'] * n_rows,
        'orgunitlevel3': ['LGA A', 'LGA B'] * (n_rows // 2),
        'Ward': [f'Ward{i%5}' for i in range(n_rows)],
        'organisationunit0me': [f'Facility{i}' for i in range(n_rows)],  # mojibake
        'period0me': [2020] * (n_rows // 2) + [2021] * (n_rows // 2),    # mojibake
        'Facility level': ['primary'] * n_rows,
        # Duplicate: one active in 2020, other active in 2021 (complementary)
        'Persons presenting with fever & tested by RDT <5yrs':   [10] * (n_rows // 2) + [0] * (n_rows // 2),
        'Persons presenting with fever & tested by RDT <5yrs.1': [0] * (n_rows // 2) + [20] * (n_rows // 2),
        'Persons tested positive for malaria by RDT <5yrs':   [5] * (n_rows // 2) + [0] * (n_rows // 2),
        'Persons tested positive for malaria by RDT <5yrs.1': [0] * (n_rows // 2) + [15] * (n_rows // 2),
    })


# --------------------------------------------------------------------------- #
#  Test 1: Non-DHIS2 passes through
# --------------------------------------------------------------------------- #

def test_non_dhis2_passes_through():
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    cleaned, report = clean_dhis2_export(df, mode='full')
    assert report.cleaning_applied is False
    pd.testing.assert_frame_equal(df, cleaned)


# --------------------------------------------------------------------------- #
#  Test 2: Legitimate dotted column names not flagged
# --------------------------------------------------------------------------- #

def test_legitimate_dotted_names_not_flagged():
    df = pd.DataFrame({
        'orgunitlevel2': ['A', 'B'],
        'orgunitlevel3': ['X', 'Y'],
        'periodname': [2024, 2024],
        'rdt_tested': [10, 20],
        'malaria_positive': [5, 10],
        'version_1.0': [1, 2],
        'version_1.1': [3, 4],   # not a pandas dup — version_1 doesn't exist as base
    })
    cleaned, _ = clean_dhis2_export(df, mode='full')
    assert 'version_1.0' in cleaned.columns
    assert 'version_1.1' in cleaned.columns


# --------------------------------------------------------------------------- #
#  Test 3: Identical duplicates dropped
# --------------------------------------------------------------------------- #

def test_identical_duplicates_dropped():
    df = pd.DataFrame({
        'orgunitlevel2': ['A', 'B'],
        'orgunitlevel3': ['X', 'Y'],
        'periodname': [2024, 2024],
        'rdt_tested': [10, 20],
        'rdt_tested.1': [10, 20],  # exact copy
        'malaria_positive': [5, 10],
    })
    cleaned, report = clean_dhis2_export(df, mode='full')
    assert list(cleaned.columns).count('rdt_tested') == 1
    assert 'rdt_tested.1' not in cleaned.columns
    assert cleaned['rdt_tested'].tolist() == [10, 20]
    assert report.duplicates_merged[0]['strategy'] == 'drop_extra'
    assert 'identical' in report.duplicates_merged[0]['reason']


# --------------------------------------------------------------------------- #
#  Test 4: Temporal dominance triggers sum
# --------------------------------------------------------------------------- #

def test_temporal_dominance_triggers_sum():
    df = pd.DataFrame({
        'orgunitlevel2': ['A'] * 6,
        'orgunitlevel3': ['X'] * 6,
        'periodname': [2020, 2020, 2021, 2021, 2022, 2022],
        'rdt_tested':   [10, 20, 0, 0, 0, 0],    # only 2020
        'rdt_tested.1': [5, 5, 30, 30, 40, 40],  # all 3 years
        'malaria_positive': [5, 10, 15, 15, 20, 20],
    })
    cleaned, report = clean_dhis2_export(df, mode='full')
    assert 'rdt_tested.1' not in cleaned.columns
    assert cleaned.loc[0, 'rdt_tested'] == 15.0  # 10 + 5
    assert cleaned.loc[2, 'rdt_tested'] == 30.0  # 0 + 30
    assert report.duplicates_merged[0]['strategy'] == 'sum'
    assert 'period coverage' in report.duplicates_merged[0]['reason']


# --------------------------------------------------------------------------- #
#  Test 5: Ratio columns use combine_first
# --------------------------------------------------------------------------- #

def test_ratio_columns_use_combine_first():
    df = pd.DataFrame({
        'orgunitlevel2': ['A', 'B'],
        'orgunitlevel3': ['X', 'Y'],
        'periodname': [2024, 2024],
        'rdt_tested': [100, 200],
        'malaria_positive': [50, 150],
        'tpr_rate':   [50.0, None],
        'tpr_rate.1': [None, 75.0],
    })
    cleaned, report = clean_dhis2_export(df, mode='full')
    assert cleaned['tpr_rate'].tolist() == [50.0, 75.0]
    assert report.duplicates_merged[0]['strategy'] == 'combine_first'


# --------------------------------------------------------------------------- #
#  Test 6: Mojibake fix
# --------------------------------------------------------------------------- #

def test_mojibake_fix():
    df = _kwara_like_fixture()
    cleaned, report = clean_dhis2_export(df, mode='full')
    assert 'periodname' in cleaned.columns
    assert 'organisationunitname' in cleaned.columns
    assert 'period0me' not in cleaned.columns
    assert 'organisationunit0me' not in cleaned.columns
    assert len(report.mojibake_fixed) == 2
    assert 'period0me' in report.column_rename_map
    assert report.column_rename_map['period0me'] == 'periodname'


# --------------------------------------------------------------------------- #
#  Test 7: NaN preservation in sum
# --------------------------------------------------------------------------- #

def test_nan_preserved_when_all_sources_nan():
    df = pd.DataFrame({
        'orgunitlevel2': ['A'] * 3,
        'orgunitlevel3': ['X'] * 3,
        'periodname': [2024, 2024, 2024],
        'rdt_tested':   [10.0, None, None],
        'rdt_tested.1': [5.0,  None, 20.0],
        'malaria_positive': [5, 10, 15],
    })
    cleaned, _ = clean_dhis2_export(df, mode='full')
    result = cleaned['rdt_tested'].tolist()
    assert result[0] == 15.0           # 10 + 5
    assert pd.isna(result[1])          # both sources NaN → NaN preserved
    assert result[2] == 20.0           # None + 20 (fillna(0).sum); NaN only preserved when ALL sources NaN


# --------------------------------------------------------------------------- #
#  Test 8: Row count preserved
# --------------------------------------------------------------------------- #

def test_row_count_preserved():
    df = _kwara_like_fixture(n_rows=50)
    cleaned, _ = clean_dhis2_export(df, mode='full')
    assert len(cleaned) == 50


# --------------------------------------------------------------------------- #
#  Test 9: Three-column duplicate group
# --------------------------------------------------------------------------- #

def test_three_column_duplicate_group():
    df = pd.DataFrame({
        'orgunitlevel2': ['A'] * 9,
        'orgunitlevel3': ['X'] * 9,
        'periodname': [2020] * 3 + [2021] * 3 + [2022] * 3,
        'rdt_tested':   [10] * 3 + [0] * 3 + [0] * 3,   # only 2020
        'rdt_tested.1': [0] * 3 + [20] * 3 + [0] * 3,   # only 2021
        'rdt_tested.2': [0] * 3 + [0] * 3 + [30] * 3,   # only 2022
        'malaria_positive': [5] * 9,
    })
    cleaned, report = clean_dhis2_export(df, mode='full')
    assert 'rdt_tested' in cleaned.columns
    assert 'rdt_tested.1' not in cleaned.columns
    assert 'rdt_tested.2' not in cleaned.columns
    assert cleaned.loc[cleaned['periodname'] == 2020, 'rdt_tested'].sum() == 30
    assert cleaned.loc[cleaned['periodname'] == 2021, 'rdt_tested'].sum() == 60
    assert cleaned.loc[cleaned['periodname'] == 2022, 'rdt_tested'].sum() == 90


# --------------------------------------------------------------------------- #
#  Test 10: Ratio columns with non-overlapping values combine cleanly
# --------------------------------------------------------------------------- #

def test_ratio_columns_non_overlapping_combines():
    df = pd.DataFrame({
        'orgunitlevel2': ['A'] * 4,
        'orgunitlevel3': ['X'] * 4,
        'periodname': [2020, 2020, 2021, 2021],
        'rdt_tested': [100] * 4,
        'malaria_positive': [75, 80, 70, 65],
        'tpr':   [75.0, 80.0, None, None],   # 2020 only
        'tpr.1': [None, None, 70.0, 65.0],   # 2021 only
    })
    cleaned, report = clean_dhis2_export(df, mode='full')
    assert cleaned['tpr'].tolist() == [75.0, 80.0, 70.0, 65.0]
    # No conflict warning because no row-level conflict
    conflicts = [w for w in report.data_quality_warnings
                 if w.get('type') == 'ratio_column_conflict']
    assert len(conflicts) == 0


# --------------------------------------------------------------------------- #
#  Test 11: Ratio columns with conflicting values emits warning
# --------------------------------------------------------------------------- #

def test_ratio_columns_with_conflict_warns():
    df = pd.DataFrame({
        'orgunitlevel2': ['A'] * 3,
        'orgunitlevel3': ['X'] * 3,
        'periodname': [2020, 2020, 2021],
        'tpr':   [75.0, 80.0, 70.0],
        'tpr.1': [76.0, 81.0, 71.0],  # different values in same rows
        'rdt_tested': [100] * 3,
        'malaria_positive': [75, 80, 70],
    })
    cleaned, report = clean_dhis2_export(df, mode='full')
    # combine_first keeps first non-null
    assert cleaned['tpr'].tolist() == [75.0, 80.0, 70.0]
    # Warning emitted
    conflicts = [w for w in report.data_quality_warnings
                 if w.get('type') == 'ratio_column_conflict']
    assert len(conflicts) == 1
    assert conflicts[0]['conflicting_rows'] == 3


# --------------------------------------------------------------------------- #
#  Test 11a: Schema coordination invariant
# --------------------------------------------------------------------------- #

def test_schema_coordination_invariant():
    """After cleaning, every schema column must exist in the cleaned df."""
    df = _kwara_like_fixture(n_rows=10)

    # Pre-cleaner schema (what LLM would return, pointing to mojibake names)
    schema = {
        'header_row': 1,
        'period': 'period0me',
        'facility_name': 'organisationunit0me',
        'ward': 'Ward',
        'state': 'orgunitlevel2',
        'lga': 'orgunitlevel3',
        'facility_level': 'Facility level',
        'u5_rdt_tested': 'Persons presenting with fever & tested by RDT <5yrs',
        'u5_rdt_positive': 'Persons tested positive for malaria by RDT <5yrs',
    }

    cleaned, report = clean_dhis2_export(df, mode='full')
    updated_schema = apply_rename_map_to_schema(schema, report.column_rename_map)

    # Every schema column must exist in cleaned df
    for field_name, col_name in updated_schema.items():
        if field_name == 'header_row' or col_name is None:
            continue
        assert col_name in cleaned.columns, (
            f"Schema[{field_name}] = '{col_name}' not found. "
            f"Columns: {list(cleaned.columns)}"
        )

    # Mojibake fields updated
    assert updated_schema['period'] == 'periodname'
    assert updated_schema['facility_name'] == 'organisationunitname'

    # Duplicate-merged field base name preserved
    assert updated_schema['u5_rdt_tested'] == 'Persons presenting with fever & tested by RDT <5yrs'

    # Merged values correct: each period gets sum of col_A + col_B
    # For 10 rows (5 in 2020, 5 in 2021): 2020 has 10*5=50, 2021 has 20*5=100
    tested_col = 'Persons presenting with fever & tested by RDT <5yrs'
    assert cleaned.loc[cleaned['periodname'] == 2020, tested_col].sum() == 50
    assert cleaned.loc[cleaned['periodname'] == 2021, tested_col].sum() == 100


# --------------------------------------------------------------------------- #
#  Test 12: Detection fires but no duplicates (Adamawa-like)
# --------------------------------------------------------------------------- #

def test_dhis2_detected_but_clean_passthrough():
    df = pd.DataFrame({
        'orgunitlevel2': ['A', 'B'],
        'orgunitlevel3': ['X', 'Y'],
        'Ward': ['W1', 'W2'],
        'periodname': [2024, 2024],
        'Facility level': ['primary', 'primary'],
        'Persons presenting with fever & tested by RDT <5yrs': [10, 20],
        'Persons tested positive for malaria by RDT <5yrs': [5, 10],
    })
    cleaned, report = clean_dhis2_export(df, mode='full')
    assert report.cleaning_applied is True  # DHIS2 detected
    assert len(report.duplicates_merged) == 0
    assert len(report.mojibake_fixed) == 0
    assert cleaned.shape == df.shape
    assert list(cleaned.columns) == list(df.columns)


# --------------------------------------------------------------------------- #
#  Test 13: Integer period codes
# --------------------------------------------------------------------------- #

def test_integer_period_codes():
    df = pd.DataFrame({
        'orgunitlevel2': ['A'] * 9,
        'orgunitlevel3': ['X'] * 9,
        'periodcode': [20240101, 20240201, 20240301] * 3,
        'rdt_tested':   [10, 0, 0] * 3,
        'rdt_tested.1': [0, 20, 30] * 3,
        'malaria_positive': [5] * 9,
    })
    cleaned, _ = clean_dhis2_export(df, mode='full')
    assert 'rdt_tested.1' not in cleaned.columns


# --------------------------------------------------------------------------- #
#  Test 14: Non-DHIS2 file with duplicate-pattern columns
# --------------------------------------------------------------------------- #

def test_non_dhis2_file_with_duplicates_untouched():
    df = pd.DataFrame({
        'facility_id': [1, 2, 3],
        'num_beds': [10, 20, 30],
        'num_beds.1': [5, 5, 5],  # looks like dup but not DHIS2
        'region': ['north', 'south', 'east'],
    })
    cleaned, report = clean_dhis2_export(df, mode='full')
    assert report.cleaning_applied is False
    pd.testing.assert_frame_equal(df, cleaned)


# --------------------------------------------------------------------------- #
#  Test 15: Cleaner exception falls back to original
# --------------------------------------------------------------------------- #

def test_cleaner_exception_falls_back_to_original():
    df = _kwara_like_fixture()
    with mock.patch.object(cleaner, 'merge_group', side_effect=RuntimeError('boom')):
        cleaned, report = clean_dhis2_export(df, mode='full')
    assert report.cleaning_applied is False
    assert report.fallback_reason is not None
    assert 'boom' in report.fallback_reason
    pd.testing.assert_frame_equal(df, cleaned)


# --------------------------------------------------------------------------- #
#  Test 16: Log-only mode returns original but writes report
# --------------------------------------------------------------------------- #

def test_log_only_mode():
    df = _kwara_like_fixture()
    cleaned, report = clean_dhis2_export(df, mode='log_only')
    # Original returned
    assert cleaned is df
    # But report shows detection and would-be changes
    assert report.cleaning_applied is True
    assert len(report.duplicates_merged) > 0
    assert report.mode == 'log_only'


# --------------------------------------------------------------------------- #
#  Test 17: Mode helper parses env var
# --------------------------------------------------------------------------- #

def test_get_cleaner_mode_parsing():
    orig = os.environ.get('CHATMRPT_DHIS2_CLEANER')
    try:
        for val, expected in [
            ('off', 'off'),
            ('true', 'full'),
            ('True', 'full'),
            ('1', 'full'),
            ('yes', 'full'),
            ('full', 'full'),
            ('log_only', 'log_only'),
            ('', 'off'),
        ]:
            os.environ['CHATMRPT_DHIS2_CLEANER'] = val
            assert get_cleaner_mode() == expected, f"Failed for {val}"

        # Default when unset
        del os.environ['CHATMRPT_DHIS2_CLEANER']
        assert get_cleaner_mode() == 'off'
    finally:
        if orig is not None:
            os.environ['CHATMRPT_DHIS2_CLEANER'] = orig
        else:
            os.environ.pop('CHATMRPT_DHIS2_CLEANER', None)


# --------------------------------------------------------------------------- #
#  Test 18: Raw upload file selector
# --------------------------------------------------------------------------- #

def test_select_raw_upload_file():
    with tempfile.TemporaryDirectory() as tmp:
        xls = os.path.join(tmp, 'raw.xls')
        csv_user = os.path.join(tmp, 'mydata.csv')
        csv_intermediate = os.path.join(tmp, 'uploaded_data.csv')
        csv_derived = os.path.join(tmp, 'unified_dataset.csv')

        open(xls, 'w').close()
        open(csv_user, 'w').close()
        open(csv_intermediate, 'w').close()
        open(csv_derived, 'w').close()

        # XLS wins over all
        assert _select_raw_upload_file([xls, csv_user, csv_intermediate]) == xls

        # Without XLS, user CSV wins over intermediates
        assert _select_raw_upload_file([csv_user, csv_intermediate, csv_derived]) == csv_user

        # Only intermediates → raise
        with pytest.raises(FileNotFoundError):
            _select_raw_upload_file([csv_intermediate, csv_derived])


# --------------------------------------------------------------------------- #
#  Test 19: Apply rename map to schema
# --------------------------------------------------------------------------- #

def test_apply_rename_map_to_schema():
    schema = {
        'header_row': 1,
        'period': 'period0me',
        'ward': 'Ward',
        'facility_name': 'organisationunit0me',
        'other': None,
    }
    rename_map = {
        'period0me': 'periodname',
        'organisationunit0me': 'organisationunitname',
    }
    updated = apply_rename_map_to_schema(schema, rename_map)
    assert updated['period'] == 'periodname'
    assert updated['facility_name'] == 'organisationunitname'
    assert updated['ward'] == 'Ward'
    assert updated['header_row'] == 1
    assert updated['other'] is None

    # Empty rename_map returns copy unchanged
    updated2 = apply_rename_map_to_schema(schema, {})
    assert updated2 == schema
