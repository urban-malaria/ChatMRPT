"""
Data loading utilities for the Data Analysis Agent.

Extracted from agent.py — loads the most comprehensive dataset
available for a given session.
"""

import os
import logging
import time
from typing import List, Dict, Any

import pandas as pd

from .encoding_handler import EncodingHandler

logger = logging.getLogger(__name__)


def get_input_data(session_id: str) -> List[Dict[str, Any]]:
    """Load most comprehensive dataset available for data-aware responses."""
    logger.info(f"=" * 100)
    logger.info(f"[_GET_INPUT_DATA] 📂 LOADING MOST COMPREHENSIVE DATA")
    logger.info(f"[_GET_INPUT_DATA] Session: {session_id}")
    logger.info(f"=" * 100)

    input_data_list = []

    session_folder = f"instance/uploads/{session_id}"
    logger.info(f"[_GET_INPUT_DATA STEP 1] Session folder: {session_folder}")
    logger.info(f"[_GET_INPUT_DATA STEP 1] Absolute path: {os.path.abspath(session_folder)}")
    logger.info(f"[_GET_INPUT_DATA STEP 1] Folder exists: {os.path.exists(session_folder)}")

    if os.path.exists(session_folder):
        files_in_folder = os.listdir(session_folder)
        logger.info(f"[_GET_INPUT_DATA STEP 1] Files in folder: {files_in_folder}")
    else:
        logger.error(f"[_GET_INPUT_DATA STEP 1] ❌ Session folder does not exist!")

    # 🔧 FIX: Check if risk analysis is complete before loading unified dataset
    # This prevents loading incomplete unified_dataset.csv without rankings
    analysis_complete_flag = os.path.join(session_folder, '.analysis_complete')
    analysis_complete = os.path.exists(analysis_complete_flag)

    logger.info(f"[_GET_INPUT_DATA STEP 2] Analysis complete flag exists: {analysis_complete}")

    # SMART DATA LOADING: Load most comprehensive dataset in priority order
    # This ensures agent always has best available context for data-aware responses
    if analysis_complete:
        # After risk analysis: prioritize unified dataset with rankings
        file_patterns = [
            'unified_dataset.csv',      # After risk analysis (MOST COMPLETE: TPR + env + rankings)
            'raw_data.csv',             # After TPR workflow (TPR + environmental variables)
            'tpr_results.csv',          # After TPR calculation (ward-level TPR)
            'data_analysis.csv',        # Alternative data file
            'uploaded_data.csv'         # Initial upload (raw facility-level data)
        ]
        logger.info(f"[_GET_INPUT_DATA STEP 2] Mode: POST-ANALYSIS (prioritize unified_dataset.csv)")
    else:
        # Before risk analysis: use raw_data.csv with TPR + environmental
        file_patterns = [
            'raw_data.csv',             # After TPR workflow (TPR + environmental variables) ← PRIORITY
            'tpr_results.csv',          # After TPR calculation (ward-level TPR)
            'data_analysis.csv',        # Alternative data file
            'uploaded_data.csv'         # Initial upload (raw facility-level data)
            # NOTE: unified_dataset.csv excluded - will be created after risk analysis
        ]
        logger.info(f"[_GET_INPUT_DATA STEP 2] Mode: PRE-ANALYSIS (use raw_data.csv, skip unified_dataset)")

    logger.info(f"[_GET_INPUT_DATA STEP 2] Smart loading - priority order: {file_patterns}")
    logger.info(f"[_GET_INPUT_DATA STEP 2] Strategy: Load MOST comprehensive dataset available")

    for idx, pattern in enumerate(file_patterns, 1):
        csv_path = os.path.join(session_folder, pattern)
        logger.info(f"[_GET_INPUT_DATA STEP 2.{idx}] Checking: {pattern}")
        logger.info(f"[_GET_INPUT_DATA STEP 2.{idx}] Full path: {csv_path}")
        logger.info(f"[_GET_INPUT_DATA STEP 2.{idx}] Exists: {os.path.exists(csv_path)}")

        if os.path.exists(csv_path):
            logger.info(f"[_GET_INPUT_DATA STEP 2.{idx}] ✅ File found: {pattern}")
            logger.info(f"[_GET_INPUT_DATA STEP 2.{idx}] File size: {os.path.getsize(csv_path)} bytes")

            try:
                logger.info(f"[_GET_INPUT_DATA STEP 3] 📊 Loading CSV with EncodingHandler...")
                load_start = time.time()
                df = EncodingHandler.read_csv_with_encoding(csv_path)
                load_time = time.time() - load_start
                logger.info(f"[_GET_INPUT_DATA STEP 3] ✅ CSV loaded in {load_time:.2f}s")
                logger.info(f"[_GET_INPUT_DATA STEP 3] DataFrame shape: {df.shape}")
                logger.info(f"[_GET_INPUT_DATA STEP 3] DataFrame columns: {df.columns.tolist()[:10]}")

                # Prepare data object (use 'data' key to match python_tool expectations)
                logger.info(f"[_GET_INPUT_DATA STEP 4] Creating data object...")
                logger.info(f"[_GET_INPUT_DATA STEP 4] ⚠️  CRITICAL: Storing DataFrame in 'data' key")
                data_obj = {
                    'variable_name': 'df',
                    'data_description': f"Dataset with {len(df)} rows and {len(df.columns)} columns",
                    'data': df,  # CRITICAL: This stores the actual DataFrame!
                    'columns': df.columns.tolist()
                }
                logger.info(f"[_GET_INPUT_DATA STEP 4] ✅ Data object created")
                logger.info(f"[_GET_INPUT_DATA STEP 4] Data object keys: {list(data_obj.keys())}")
                logger.info(f"[_GET_INPUT_DATA STEP 4] Data object 'data' type: {type(data_obj['data'])}")

                input_data_list.append(data_obj)
                logger.info(f"[_GET_INPUT_DATA STEP 5] ✅ Loaded {pattern}: {len(df)} rows, {len(df.columns)} columns")

                # Log which dataset was chosen and why
                dataset_context = {
                    'unified_dataset.csv': 'COMPREHENSIVE (TPR + environmental + risk rankings)',
                    'raw_data.csv': 'TPR + ENVIRONMENTAL (prepared for risk analysis)',
                    'tpr_results.csv': 'WARD-LEVEL TPR (after TPR workflow)',
                    'data_analysis.csv': 'ANALYSIS DATA',
                    'uploaded_data.csv': 'RAW FACILITY DATA (initial upload)'
                }
                context_desc = dataset_context.get(pattern, 'UNKNOWN')
                logger.info(f"[_GET_INPUT_DATA STEP 5] 📊 Dataset context: {context_desc}")
                logger.info(f"[_GET_INPUT_DATA STEP 5] 🎯 Agent now has access to this data for ALL questions")

                logger.info(f"=" * 100)
                logger.info(f"[_GET_INPUT_DATA] 📂 DATA LOADING COMPLETE - SUCCESS")
                logger.info(f"=" * 100)
                break

            except Exception as e:
                logger.error(f"[_GET_INPUT_DATA STEP 3] ❌ Error loading {pattern}: {e}", exc_info=True)
                continue

    # Also load time-series data if available (alongside main dataset for trend analysis)
    _supplemental = {
        'ts_df':    'tpr_time_series.csv',
        'trend_df': 'trend_summary.csv',
    }
    for var_name, fname in _supplemental.items():
        sup_path = os.path.join(session_folder, fname)
        if os.path.exists(sup_path):
            try:
                sup_df = EncodingHandler.read_csv_with_encoding(sup_path)
                input_data_list.append({
                    'variable_name': var_name,
                    'data_description': f"{fname} ({len(sup_df)} rows, columns: {sup_df.columns.tolist()})",
                    'data': sup_df,
                    'columns': sup_df.columns.tolist()
                })
                logger.info(f"[_GET_INPUT_DATA] Loaded {var_name} from {fname}: {sup_df.shape}")
            except Exception as e:
                logger.warning(f"[_GET_INPUT_DATA] Could not load {fname}: {e}")

    if not input_data_list:
        logger.warning(f"[_GET_INPUT_DATA] ⚠️  No datasets loaded via priority list. Falling back to scan session folder.")

        try:
            all_files = sorted(os.listdir(session_folder)) if os.path.exists(session_folder) else []
        except Exception as exc:
            logger.error(f"[_GET_INPUT_DATA FALLBACK] Unable to list session folder: {exc}")
            all_files = []

        # Load saved header_row from schema (e.g. DHIS2 XLS has blank row 0, headers in row 1)
        _fallback_header_row = 0
        try:
            from app.agent.state_manager import DataAnalysisStateManager
            _saved_state = DataAnalysisStateManager(session_id).load_state() or {}
            _fallback_header_row = (_saved_state.get('column_schema') or {}).get('header_row', 0)
        except Exception:
            pass

        fallback_extensions = ('.csv', '.xlsx', '.xls')
        for fname in all_files:
            if not fname.lower().endswith(fallback_extensions):
                continue

            fallback_path = os.path.join(session_folder, fname)
            logger.info(f"[_GET_INPUT_DATA FALLBACK] Attempting to load {fallback_path}")

            try:
                if fname.lower().endswith('.csv'):
                    df = EncodingHandler.read_csv_with_encoding(fallback_path)
                else:
                    # Use header_row from schema inferred at upload time.
                    # If schema inference failed at upload, this defaults to 0.
                    logger.info(f"[_GET_INPUT_DATA FALLBACK] Reading Excel with header_row={_fallback_header_row}")
                    df = EncodingHandler.read_excel_with_encoding(fallback_path, header=_fallback_header_row)

                data_obj = {
                    'variable_name': 'df',
                    'data_description': f"Dataset with {len(df)} rows and {len(df.columns)} columns",
                    'data': df,
                    'columns': df.columns.tolist()
                }
                input_data_list.append(data_obj)
                logger.info(f"[_GET_INPUT_DATA FALLBACK] ✅ Loaded {fname}: {df.shape}")
                break
            except Exception as exc:
                logger.error(f"[_GET_INPUT_DATA FALLBACK] Error loading {fname}: {exc}", exc_info=True)
                continue

    if not input_data_list:
        logger.warning(f"[_GET_INPUT_DATA] ⚠️  No datasets loaded after fallback.")

    logger.info(f"[_GET_INPUT_DATA FINAL] Total datasets loaded: {len(input_data_list)}")
    return input_data_list
