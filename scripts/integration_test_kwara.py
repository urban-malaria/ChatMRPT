#!/usr/bin/env python3
"""
Integration test: DHIS2 cleaner on the full Kwara pipeline.

Runs the cleaner on a real Kwara XLS file and verifies:
  1. DHIS2 detection fires
  2. 4 duplicate groups merged
  3. Mojibake column renames applied
  4. Schema invariant holds after rename-map application
  5. (Optional) TPR calculation produces 6-year data if imports succeed

Usage:
  python scripts/integration_test_kwara.py [/path/to/kwara.xls]

Exit code 0 on success, 1 on any check failure.
"""

import os
import sys
import importlib.util
import types

# Bypass flask import chain
if 'app' not in sys.modules:
    sys.modules['app'] = types.ModuleType('app')
if 'app.utils' not in sys.modules:
    sys.modules['app.utils'] = types.ModuleType('app.utils')


def _load_cleaner():
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)

    spec1 = importlib.util.spec_from_file_location(
        'app.utils.dhis2_mojibake_patterns',
        os.path.join(root, 'app', 'utils', 'dhis2_mojibake_patterns.py')
    )
    mojibake = importlib.util.module_from_spec(spec1)
    sys.modules['app.utils.dhis2_mojibake_patterns'] = mojibake
    spec1.loader.exec_module(mojibake)

    spec2 = importlib.util.spec_from_file_location(
        'app.utils.dhis2_cleaner',
        os.path.join(root, 'app', 'utils', 'dhis2_cleaner.py')
    )
    cleaner = importlib.util.module_from_spec(spec2)
    sys.modules['app.utils.dhis2_cleaner'] = cleaner
    spec2.loader.exec_module(cleaner)
    return cleaner


def main():
    import pandas as pd

    cleaner = _load_cleaner()

    # Locate Kwara XLS
    kwara_path = sys.argv[1] if len(sys.argv) > 1 else None
    if not kwara_path:
        candidates = [
            'data/datasets/Kwara TPR data 2020 - 2025 ~ 2026-03-02 (1).xls',
            'instance/uploads/d0a7b4a0-b7b3-4310-858f-ccf4355c5420/uploaded_data.csv',
        ]
        for c in candidates:
            if os.path.exists(c):
                kwara_path = c
                break

    if not kwara_path or not os.path.exists(kwara_path):
        print(f"❌ Kwara file not found. Pass path as argument or place at:")
        for c in candidates:
            print(f"   {c}")
        return 1

    print(f"Loading: {kwara_path}")
    if kwara_path.lower().endswith(('.xls', '.xlsx')):
        df = pd.read_excel(kwara_path, header=1)
    else:
        df = pd.read_csv(kwara_path)
    print(f"Input shape: {df.shape}")

    checks = []

    # --- Run cleaner ---
    cleaned, report = cleaner.clean_dhis2_export(df, mode='full')

    checks.append(("Cleaner applied", report.cleaning_applied is True))
    checks.append(("Detected as DHIS2", report.detected_as is not None))
    checks.append(("Duplicates detected", len(report.duplicates_merged) >= 4))
    checks.append(("Mojibake fixed", len(report.mojibake_fixed) >= 2))
    checks.append(("Column count reduced", cleaned.shape[1] < df.shape[1]))
    checks.append(("Row count preserved", cleaned.shape[0] == df.shape[0]))

    # --- Schema coordination invariant ---
    schema = {
        'header_row': 1,
        'state': 'orgunitlevel2',
        'lga': 'orgunitlevel3',
        'ward': 'Ward',
        'facility_name': 'organisationunit0me',
        'facility_level': 'Facility level',
        'period': 'period0me',
        'u5_rdt_tested': 'Persons presenting with fever & tested by RDT <5yrs',
        'u5_rdt_positive': 'Persons tested positive for malaria by RDT <5yrs',
    }
    updated_schema = cleaner.apply_rename_map_to_schema(schema, report.column_rename_map)

    all_valid = True
    for field, col in updated_schema.items():
        if field == 'header_row' or col is None:
            continue
        if col not in cleaned.columns:
            all_valid = False
            break
    checks.append(("Schema invariant holds", all_valid))

    # --- Verify period column fix ---
    checks.append(
        ("Period column renamed (period0me → periodname)",
         updated_schema['period'] == 'periodname')
    )

    # --- Verify TPR calculation can run (guarded imports — not a hard fail) ---
    try:
        from app.tpr.utils import calculate_ward_tpr_timeseries
        _tpr_ok = True
    except ImportError as exc:
        print(f"⚠️  TPR module imports not available (skip TPR check): {exc}")
        _tpr_ok = False

    if _tpr_ok:
        ts_df = calculate_ward_tpr_timeseries(
            cleaned, age_group='u5', test_method='rdt',
            facility_level='primary', schema=updated_schema
        )
        checks.append(("Time series produced output", len(ts_df) > 0))
        if len(ts_df) > 0:
            years = sorted(ts_df['Period'].unique())
            checks.append(("Time series spans 2020-2025", len(years) == 6))

    # --- Pass-through test with Adamawa ---
    adamawa_path = 'data/datasets/adamawa_tpr_cleaned.csv'
    if os.path.exists(adamawa_path):
        adf = pd.read_csv(adamawa_path)
        acleaned, areport = cleaner.clean_dhis2_export(adf, mode='full')
        checks.append(("Adamawa pass-through (shape unchanged)", acleaned.shape == adf.shape))
        checks.append(("Adamawa no duplicates merged", len(areport.duplicates_merged) == 0))

    # --- Print results ---
    print("\n=== Integration Test Results ===")
    all_passed = True
    for name, result in checks:
        status = "✅" if result else "❌"
        print(f"  {status} {name}")
        if not result:
            all_passed = False

    if all_passed:
        print("\n🎉 All integration checks PASSED")
        # Print the TPR evidence
        if _tpr_ok and 'ts_df' in locals() and len(ts_df) > 0:
            print("\nTPR time-series summary:")
            print(ts_df.groupby('Period').agg(
                total_positive=('Total_Positive', 'sum'),
                total_tested=('Total_Tested', 'sum'),
                mean_tpr=('TPR', 'mean'),
            ))
    else:
        print("\n❌ Some checks FAILED")

    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
