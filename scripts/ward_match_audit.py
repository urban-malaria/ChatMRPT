"""
Ward name matching audit script (Phase 0).

Reads an existing session folder and produces ward_match_report.json
without modifying any data files.

Usage:
    python scripts/ward_match_audit.py <session_id>
    python scripts/ward_match_audit.py <session_id> --shapefile path/to/shapefile.shp

The session folder is expected at instance/uploads/<session_id>/
and must contain tpr_time_series.csv and raw_data.csv.
"""

import sys
import os
import json
import difflib
import argparse
import logging

import pandas as pd

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.ward_matcher import normalize_ward_name

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def _seq_ratio(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()


def audit_session(session_id: str, shapefile_path: str | None = None) -> dict:
    session_folder = os.path.join('instance', 'uploads', session_id)

    ts_path  = os.path.join(session_folder, 'tpr_time_series.csv')
    raw_path = os.path.join(session_folder, 'raw_data.csv')

    if not os.path.exists(ts_path):
        logger.error(f"tpr_time_series.csv not found in {session_folder}")
        sys.exit(1)
    if not os.path.exists(raw_path):
        logger.error(f"raw_data.csv not found in {session_folder}")
        sys.exit(1)

    ts_df  = pd.read_csv(ts_path)
    raw_df = pd.read_csv(raw_path)

    # ── Shapefile ward inventory ────────────────────────────────────────────
    if shapefile_path and os.path.exists(shapefile_path):
        import geopandas as gpd
        shp = gpd.read_file(shapefile_path)
        shapefile_wards = shp['WardName'].dropna().tolist()
        wardcode_in_shapefile = 'WardCode' in shp.columns
    else:
        # Fall back to raw_data.csv (already shapefile-keyed after analysis_tool merge)
        shapefile_wards = raw_df['WardName'].dropna().tolist()
        wardcode_in_shapefile = 'WardCode' in raw_df.columns

    wardcode_in_ts = 'WardCode' in ts_df.columns

    shp_norm_map: dict = {}
    for w in shapefile_wards:
        key = normalize_ward_name(w)
        shp_norm_map.setdefault(key, []).append(w)

    dup_shp = sum(1 for v in shp_norm_map.values() if len(v) > 1)

    # ── DHIS2 side ──────────────────────────────────────────────────────────
    dhis2_wards = ts_df['WardName'].dropna().unique().tolist()
    dhis2_norm_map: dict = {}
    for w in dhis2_wards:
        key = normalize_ward_name(w)
        dhis2_norm_map.setdefault(key, []).append(w)

    dup_dhis2 = sum(1 for v in dhis2_norm_map.values() if len(v) > 1)

    shp_keys  = list(shp_norm_map.keys())
    d2_keys   = list(dhis2_norm_map.keys())

    # ── Per-year breakdown ──────────────────────────────────────────────────
    years = sorted(ts_df['Period'].dropna().unique().tolist())
    per_year: dict = {}
    all_unmatched: list = []

    for year in years:
        yr_rows = ts_df[ts_df['Period'] == year]
        yr_d2   = yr_rows['WardName'].dropna().unique().tolist()
        yr_keys = [normalize_ward_name(w) for w in yr_d2]

        # Wards in shapefile absent from this year's DHIS2 data entirely
        no_data_count    = 0
        exact_count      = 0
        fuzzy_count      = 0
        unmatched_count  = 0
        ambiguous_count  = 0

        for shp_key, shp_originals in shp_norm_map.items():
            if shp_key in yr_keys:
                exact_count += 1
                continue

            # Check total_tested for this ward in source data
            # (approximate: if ward is absent from ts entirely for this year it's no-data)
            # ts_df only contains rows where total_tested > 0, so absence = no data
            # We count it as no_data only if it matched in other years
            matched_other_years = any(
                shp_key in [normalize_ward_name(w) for w in ts_df[ts_df['Period'] == y]['WardName'].tolist()]
                for y in years if y != year
            )

            # Fuzzy attempt
            matches = difflib.get_close_matches(shp_key, yr_keys, n=2, cutoff=0.70)
            if matches:
                # Check for ambiguity
                if len(matches) > 1 and _seq_ratio(shp_key, matches[0]) == _seq_ratio(shp_key, matches[1]):
                    ambiguous_count += 1
                else:
                    best  = matches[0]
                    score = _seq_ratio(shp_key, best)
                    fuzzy_count += 1
                    if matched_other_years is False:
                        # Ward never appears in timeseries — likely no data
                        no_data_count += 1
                        fuzzy_count   -= 1
            else:
                if matched_other_years:
                    unmatched_count += 1
                    original  = shp_originals[0]
                    closest   = difflib.get_close_matches(shp_key, d2_keys, n=1, cutoff=0.0)
                    all_unmatched.append({
                        'shapefile': original,
                        'shapefile_norm': shp_key,
                        'dhis2_closest': closest[0] if closest else None,
                        'score': round(_seq_ratio(shp_key, closest[0]), 3) if closest else None,
                    })
                else:
                    no_data_count += 1

        per_year[str(year)] = {
            'total_shapefile_wards': len(shp_norm_map),
            'exact_match':               exact_count,
            'fuzzy_match':               fuzzy_count,
            'no_data_total_tested_zero': no_data_count,
            'name_mismatch_unmatched':   unmatched_count,
            'ambiguous_duplicate_key':   ambiguous_count,
        }

    # ── Summary ─────────────────────────────────────────────────────────────
    report = {
        'session_id':                      session_id,
        'total_shapefile_wards':           len(shapefile_wards),
        'total_dhis2_wards_ever':          len(dhis2_wards),
        'years_analyzed':                  [str(y) for y in years],
        'wardcode_in_shapefile':           wardcode_in_shapefile,
        'wardcode_in_timeseries':          wardcode_in_ts,
        'duplicate_normalized_keys_shapefile': dup_shp,
        'duplicate_normalized_keys_dhis2':    dup_dhis2,
        'per_year':                        per_year,
        'unmatched_pairs':                 all_unmatched,
    }

    out_path = os.path.join(session_folder, 'ward_match_report.json')
    with open(out_path, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"\nReport written to {out_path}")
    logger.info(f"Total shapefile wards : {report['total_shapefile_wards']}")
    logger.info(f"WardCode available    : shapefile={wardcode_in_shapefile}, timeseries={wardcode_in_ts}")
    logger.info(f"Duplicate norm keys   : shapefile={dup_shp}, dhis2={dup_dhis2}")
    for yr, counts in per_year.items():
        logger.info(
            f"  {yr}: exact={counts['exact_match']} fuzzy={counts['fuzzy_match']} "
            f"no_data={counts['no_data_total_tested_zero']} "
            f"unmatched={counts['name_mismatch_unmatched']} "
            f"ambiguous={counts['ambiguous_duplicate_key']}"
        )
    if all_unmatched:
        logger.info(f"\nUnmatched ward pairs ({len(all_unmatched)}):")
        for pair in all_unmatched[:20]:
            logger.info(f"  shapefile='{pair['shapefile']}' closest='{pair['dhis2_closest']}' score={pair['score']}")

    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Audit ward name matching for a session.')
    parser.add_argument('session_id', help='Session ID (folder under instance/uploads/)')
    parser.add_argument('--shapefile', default=None, help='Optional path to shapefile for authoritative ward list')
    args = parser.parse_args()

    audit_session(args.session_id, args.shapefile)
