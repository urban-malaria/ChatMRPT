"""
Background service that runs composite + PCA risk analysis for each year
of a multi-year dataset, producing per-year unified_dataset_{year}.csv files.

Same daemon thread pattern as precompute_service.py — no Flask app context needed.
"""

import json
import logging
import os
import threading
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_STATUS_FILE = 'multi_year_status.json'


def _read_status(session_folder: str) -> Dict[str, Any]:
    path = os.path.join(session_folder, _STATUS_FILE)
    try:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _write_status(session_folder: str, status: Dict[str, Any]) -> None:
    path = os.path.join(session_folder, _STATUS_FILE)
    try:
        with open(path, 'w') as f:
            json.dump(status, f, indent=2)
    except Exception as e:
        logger.warning(f"[MULTI_YEAR] Could not write status file: {e}")


def _run_year(session_id: str, session_folder: str, year: int,
              state_manager) -> str:
    """
    Run composite + PCA analysis for a single year.
    Returns 'complete', 'failed', or 'skipped'.
    """
    year_tag = f'_{year}'
    raw_path = os.path.join(session_folder, f'raw_data{year_tag}.csv')
    if not os.path.exists(raw_path):
        logger.warning(f"[MULTI_YEAR] {raw_path} not found — skipping year {year}")
        return 'skipped'

    try:
        from app.services.data_handler import DataHandler
        from app.analysis.engine import AnalysisEngine

        data_handler = DataHandler(session_folder)
        data_handler.load_raw_data(year_tag=year_tag)

        engine = AnalysisEngine(data_handler=data_handler)

        # Run composite
        logger.info(f"[MULTI_YEAR] Starting composite analysis for year {year}")
        composite_result = engine.run_composite_analysis(
            session_id=session_id, year_tag=year_tag
        )
        if composite_result.get('status') == 'error':
            logger.warning(f"[MULTI_YEAR] Composite failed for {year}: {composite_result.get('message')}")
            return 'failed'

        # Run PCA (non-fatal if it fails — composite is enough for ITN)
        logger.info(f"[MULTI_YEAR] Starting PCA analysis for year {year}")
        try:
            engine.run_pca_analysis(session_id=session_id, year_tag=year_tag)
        except Exception as pca_err:
            logger.warning(f"[MULTI_YEAR] PCA skipped for {year}: {pca_err}")

        logger.info(f"[MULTI_YEAR] Year {year} complete")
        return 'complete'

    except Exception as e:
        logger.error(f"[MULTI_YEAR] Year {year} failed: {e}", exc_info=True)
        return 'failed'


def _execute_multi_year(session_id: str, years: List[int],
                        session_folder: str, state_manager) -> None:
    """Worker function run in the daemon thread."""
    detail = {str(y): 'pending' for y in years}
    detail['aggregate'] = 'pending'

    status: Dict[str, Any] = {
        'status': 'running',
        'years_total': len(years),
        'years_complete': 0,
        'detail': detail,
    }
    _write_status(session_folder, status)

    complete_count = 0

    for year in years:
        detail[str(year)] = 'running'
        _write_status(session_folder, status)

        outcome = _run_year(session_id, session_folder, year, state_manager)
        detail[str(year)] = outcome
        if outcome == 'complete':
            complete_count += 1
        status['years_complete'] = complete_count
        _write_status(session_folder, status)

    # Run aggregate last so it doesn't overwrite per-year files in memory
    logger.info("[MULTI_YEAR] Running aggregate (year_tag='') analysis")
    detail['aggregate'] = 'running'
    _write_status(session_folder, status)

    try:
        from app.services.data_handler import DataHandler
        from app.analysis.engine import AnalysisEngine

        data_handler = DataHandler(session_folder)
        data_handler.load_raw_data(year_tag='')
        engine = AnalysisEngine(data_handler=data_handler)
        engine.run_composite_analysis(session_id=session_id, year_tag='')
        try:
            engine.run_pca_analysis(session_id=session_id, year_tag='')
        except Exception:
            pass
        detail['aggregate'] = 'complete'
    except Exception as e:
        logger.error(f"[MULTI_YEAR] Aggregate analysis failed: {e}", exc_info=True)
        detail['aggregate'] = 'failed'

    all_ok = all(v in ('complete', 'skipped') for k, v in detail.items())
    status['status'] = 'complete' if all_ok else 'partial'
    status['years_complete'] = complete_count
    _write_status(session_folder, status)
    logger.info(f"[MULTI_YEAR] Background analysis done — status={status['status']}")


def schedule_multi_year_risk_analysis(session_id: str, years: List[int],
                                      session_folder: str,
                                      state_manager) -> None:
    """
    Launch the multi-year background risk analysis as a daemon thread.

    Args:
        session_id:     Session identifier
        years:          Sorted list of integer years (e.g. [2020, 2021, ..., 2025])
        session_folder: Absolute path to instance/uploads/{session_id}
        state_manager:  DataAnalysisStateManager for schema access (passed through)
    """
    thread = threading.Thread(
        target=_execute_multi_year,
        args=(session_id, years, session_folder, state_manager),
        daemon=True,
        name=f"multi-year-{session_id[:8]}",
    )
    thread.start()
    logger.info(
        f"[MULTI_YEAR] Background thread started for {len(years)} years "
        f"({years[0]}–{years[-1]}) session={session_id[:12]}"
    )
