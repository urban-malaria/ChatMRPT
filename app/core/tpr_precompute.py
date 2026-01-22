"""
TPR Pre-computation Module

Pre-computes all 16 TPR combinations (4 facility levels x 4 age groups) after
the user completes their initial selection. Stores results in SQLite for fast
retrieval in standard flow.
"""

import os
import sqlite3
import logging
import pandas as pd
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)

# All valid combinations
FACILITY_LEVELS = ['primary', 'secondary', 'tertiary', 'all']
AGE_GROUPS = ['u5', 'o5', 'pw', 'all_ages']


def get_precompute_db_path(session_id: str) -> str:
    """Get the path to the pre-computed TPR database for a session."""
    return f"instance/uploads/{session_id}/tpr_precomputed.db"


def init_precompute_db(session_id: str) -> str:
    """
    Initialize the SQLite database for storing pre-computed TPR results.

    Returns:
        Path to the database file
    """
    db_path = get_precompute_db_path(session_id)

    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table for TPR results
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tpr_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            facility_level TEXT NOT NULL,
            age_group TEXT NOT NULL,
            ward_name TEXT NOT NULL,
            lga TEXT,
            tpr REAL,
            total_tested INTEGER,
            total_positive INTEGER,
            UNIQUE(facility_level, age_group, ward_name)
        )
    ''')

    # Create index for fast lookups
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_combo
        ON tpr_results(facility_level, age_group)
    ''')

    # Create metadata table to track computation status
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS computation_status (
            facility_level TEXT NOT NULL,
            age_group TEXT NOT NULL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ward_count INTEGER,
            PRIMARY KEY (facility_level, age_group)
        )
    ''')

    conn.commit()
    conn.close()

    logger.info(f"Initialized TPR pre-compute database at {db_path}")
    return db_path


def precompute_all_tpr_combinations(
    session_id: str,
    data: pd.DataFrame,
    state: str,
    exclude_combination: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Pre-compute all 16 TPR combinations and store in SQLite.

    Args:
        session_id: Session identifier
        data: DataFrame with TPR data
        state: Selected state name
        exclude_combination: Optional dict with 'facility_level' and 'age_group'
                           to skip (already computed by user's selection)

    Returns:
        Dict with status and statistics
    """
    from app.core.tpr_utils import calculate_ward_tpr

    logger.info(f"Starting TPR pre-computation for session {session_id}, state: {state}")

    # Initialize database
    db_path = init_precompute_db(session_id)

    results = {
        'success': True,
        'combinations_computed': 0,
        'combinations_skipped': 0,
        'errors': []
    }

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for facility_level in FACILITY_LEVELS:
        for age_group in AGE_GROUPS:
            # Skip the combination user already selected (it's computed separately)
            if exclude_combination:
                if (facility_level == exclude_combination.get('facility_level') and
                    age_group == exclude_combination.get('age_group')):
                    logger.info(f"Skipping user-selected combination: {facility_level}/{age_group}")
                    results['combinations_skipped'] += 1
                    continue

            try:
                logger.info(f"Computing TPR for {facility_level}/{age_group}")

                # Calculate TPR for this combination
                tpr_df = calculate_ward_tpr(
                    df=data.copy(),
                    age_group=age_group,
                    test_method='both',
                    facility_level=facility_level
                )

                if tpr_df is None or tpr_df.empty:
                    logger.warning(f"No results for {facility_level}/{age_group}")
                    continue

                # Store results in database
                for _, row in tpr_df.iterrows():
                    cursor.execute('''
                        INSERT OR REPLACE INTO tpr_results
                        (facility_level, age_group, ward_name, lga, tpr, total_tested, total_positive)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        facility_level,
                        age_group,
                        row.get('WardName', row.get('ward_name', '')),
                        row.get('LGA', row.get('lga', '')),
                        row.get('TPR', row.get('tpr', 0)),
                        int(row.get('Total_Tested', row.get('total_tested', 0))),
                        int(row.get('Total_Positive', row.get('total_positive', 0)))
                    ))

                # Update computation status
                cursor.execute('''
                    INSERT OR REPLACE INTO computation_status
                    (facility_level, age_group, ward_count)
                    VALUES (?, ?, ?)
                ''', (facility_level, age_group, len(tpr_df)))

                conn.commit()
                results['combinations_computed'] += 1
                logger.info(f"Stored {len(tpr_df)} wards for {facility_level}/{age_group}")

            except Exception as e:
                error_msg = f"Error computing {facility_level}/{age_group}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

    conn.close()

    logger.info(f"TPR pre-computation complete: {results['combinations_computed']} computed, "
                f"{results['combinations_skipped']} skipped, {len(results['errors'])} errors")

    return results


def is_tpr_precomputed(session_id: str) -> bool:
    """Check if pre-computed TPR data exists for a session."""
    db_path = get_precompute_db_path(session_id)

    if not os.path.exists(db_path):
        return False

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM computation_status')
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


def query_precomputed_tpr(
    session_id: str,
    facility_level: str = 'all',
    age_group: str = 'all_ages',
    lga: Optional[str] = None,
    top_n: Optional[int] = None,
    sort_by: str = 'tpr',
    sort_desc: bool = True
) -> Dict[str, Any]:
    """
    Query pre-computed TPR results for a specific combination.

    Args:
        session_id: Session identifier
        facility_level: Facility level filter
        age_group: Age group filter
        lga: Optional LGA filter
        top_n: Optional limit on results
        sort_by: Column to sort by ('tpr', 'total_tested', 'ward_name')
        sort_desc: Sort descending if True

    Returns:
        Dict with results and summary statistics
    """
    db_path = get_precompute_db_path(session_id)

    if not os.path.exists(db_path):
        return {
            'success': False,
            'error': 'No pre-computed TPR data found. Please complete the TPR workflow first.',
            'data': []
        }

    # Normalize inputs
    facility_level = facility_level.lower().replace(' ', '_')
    age_group = age_group.lower().replace(' ', '_')

    # Map common variations
    age_group_map = {
        'under_5': 'u5', 'under5': 'u5', 'u5': 'u5', '<5': 'u5',
        'over_5': 'o5', 'over5': 'o5', 'o5': 'o5', '>5': 'o5', '5+': 'o5',
        'pregnant_women': 'pw', 'pregnant': 'pw', 'pw': 'pw', 'anc': 'pw',
        'all_ages': 'all_ages', 'all': 'all_ages', 'combined': 'all_ages'
    }
    age_group = age_group_map.get(age_group, age_group)

    facility_map = {
        'primary': 'primary', 'phc': 'primary',
        'secondary': 'secondary', 'shc': 'secondary',
        'tertiary': 'tertiary', 'thc': 'tertiary',
        'all': 'all', 'all_facilities': 'all'
    }
    facility_level = facility_map.get(facility_level, facility_level)

    try:
        conn = sqlite3.connect(db_path)

        # Build query
        query = '''
            SELECT ward_name, lga, tpr, total_tested, total_positive
            FROM tpr_results
            WHERE facility_level = ? AND age_group = ?
        '''
        params = [facility_level, age_group]

        if lga:
            query += ' AND LOWER(lga) LIKE ?'
            params.append(f'%{lga.lower()}%')

        # Sort
        sort_column = {
            'tpr': 'tpr',
            'total_tested': 'total_tested',
            'ward_name': 'ward_name',
            'tested': 'total_tested',
            'positive': 'total_positive'
        }.get(sort_by.lower(), 'tpr')

        order = 'DESC' if sort_desc else 'ASC'
        query += f' ORDER BY {sort_column} {order}'

        if top_n:
            query += f' LIMIT {int(top_n)}'

        df = pd.read_sql_query(query, conn, params=params)

        # Calculate summary statistics
        if not df.empty:
            summary = {
                'total_wards': len(df),
                'avg_tpr': round(df['tpr'].mean(), 2),
                'max_tpr': round(df['tpr'].max(), 2),
                'min_tpr': round(df['tpr'].min(), 2),
                'total_tested': int(df['total_tested'].sum()),
                'total_positive': int(df['total_positive'].sum()),
                'facility_level': facility_level,
                'age_group': age_group
            }
        else:
            summary = {
                'total_wards': 0,
                'facility_level': facility_level,
                'age_group': age_group,
                'note': 'No data found for this combination'
            }

        conn.close()

        return {
            'success': True,
            'data': df.to_dict('records'),
            'summary': summary
        }

    except Exception as e:
        logger.error(f"Error querying pre-computed TPR: {e}")
        return {
            'success': False,
            'error': str(e),
            'data': []
        }


def get_available_combinations(session_id: str) -> List[Dict[str, str]]:
    """Get list of all pre-computed combinations for a session."""
    db_path = get_precompute_db_path(session_id)

    if not os.path.exists(db_path):
        return []

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT facility_level, age_group, ward_count, computed_at
            FROM computation_status
            ORDER BY facility_level, age_group
        ''')
        rows = cursor.fetchall()
        conn.close()

        return [
            {
                'facility_level': row[0],
                'age_group': row[1],
                'ward_count': row[2],
                'computed_at': row[3]
            }
            for row in rows
        ]
    except Exception:
        return []
