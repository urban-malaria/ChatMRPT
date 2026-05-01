"""
WhatsApp upload processor.

Saves a file received via WhatsApp, runs schema inference and DHIS2
cleaning, and builds a MetadataCache entry — all without touching
Flask's request-scoped session.  Must be called inside app.app_context().
"""

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ALLOWED_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
_CONTENT_TYPE_EXT = {
    'text/csv': '.csv',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
    'application/vnd.ms-excel': '.xls',
}


def _ext_from_content_type(content_type: str) -> str:
    return _CONTENT_TYPE_EXT.get(content_type, '')


def process_whatsapp_upload(
    file_bytes: bytes,
    filename: str,
    content_type: str,
    session_id: str,
    app,
) -> dict[str, Any]:
    """
    Save and process a user-uploaded file received via WhatsApp.

    Must be called inside ``with app.app_context()``.

    Returns a dict with keys:
        rows, cols, detected_type, key_columns, cleaning_report, error
    """
    result: dict[str, Any] = {
        'rows': 0,
        'cols': 0,
        'detected_type': 'unknown',
        'key_columns': [],
        'cleaning_report': None,
        'error': None,
    }

    # ------------------------------------------------------------------ #
    #  1. Validate file type
    # ------------------------------------------------------------------ #
    ext = Path(filename).suffix.lower()
    if not ext:
        ext = _ext_from_content_type(content_type)

    if ext not in _ALLOWED_EXTENSIONS:
        result['error'] = (
            f"Unsupported file type '{ext or content_type}'. "
            "Please send a CSV or Excel file (.csv / .xlsx / .xls)."
        )
        logger.warning(f'WhatsApp upload rejected: unsupported type {ext or content_type}')
        return result

    # Normalise filename to have correct extension
    if not Path(filename).suffix:
        filename = f'upload{ext}'

    # ------------------------------------------------------------------ #
    #  2. Write bytes to disk
    # ------------------------------------------------------------------ #
    upload_dir = Path(app.instance_path) / 'uploads' / session_id
    upload_dir.mkdir(parents=True, exist_ok=True)

    filepath = upload_dir / filename
    filepath.write_bytes(file_bytes)
    logger.info(f'Saved WhatsApp upload: {filepath} ({len(file_bytes):,} bytes)')

    # Also write as data_analysis.csv / data_analysis.xlsx for pipeline compat
    analysis_name = f'data_analysis{ext}'
    (upload_dir / analysis_name).write_bytes(file_bytes)

    filepath_str = str(filepath)

    # ------------------------------------------------------------------ #
    #  3. Schema inference (LLM call)
    # ------------------------------------------------------------------ #
    try:
        from app.tpr.data_analyzer import TPRDataAnalyzer
        analyzer = TPRDataAnalyzer()
        df, schema = analyzer.infer_schema_from_file(filepath_str)
    except Exception as exc:
        logger.exception('Schema inference failed')
        result['error'] = f"Could not read your file: {exc}"
        return result

    result['rows'] = int(df.shape[0])
    result['cols'] = int(df.shape[1])

    # ------------------------------------------------------------------ #
    #  4. DHIS2 cleaning
    # ------------------------------------------------------------------ #
    try:
        from app.utils.dhis2_cleaner import clean_dhis2_export, get_cleaner_mode
        mode = get_cleaner_mode()
        cleaned_df, report = clean_dhis2_export(df, mode=mode)
        result['detected_type'] = report.detected_as or 'tabular data'
        result['cleaning_report'] = report
    except Exception:
        logger.exception('DHIS2 cleaning failed — using raw dataframe')
        cleaned_df = df
        result['detected_type'] = 'tabular data'

    # ------------------------------------------------------------------ #
    #  5. Save cleaned dataframe as uploaded_data.csv
    # ------------------------------------------------------------------ #
    try:
        cleaned_df.to_csv(upload_dir / 'uploaded_data.csv', index=False)
    except Exception:
        logger.exception('Failed to save uploaded_data.csv')

    # ------------------------------------------------------------------ #
    #  6. Build MetadataCache entry
    # ------------------------------------------------------------------ #
    try:
        from app.agent.metadata_cache import MetadataCache
        uploaded_data_path = str(upload_dir / 'uploaded_data.csv')
        metadata = MetadataCache.extract_file_metadata(uploaded_data_path, 'uploaded_data.csv')
        MetadataCache.save_cache(session_id, metadata)
    except Exception:
        logger.exception('MetadataCache build failed')

    # ------------------------------------------------------------------ #
    #  7. Extract key columns for the summary message
    # ------------------------------------------------------------------ #
    result['key_columns'] = list(cleaned_df.columns[:8])

    logger.info(
        f'WhatsApp upload processed: session={session_id} '
        f'rows={result["rows"]} cols={result["cols"]} '
        f'type={result["detected_type"]}'
    )
    return result


def build_summary_messages(filename: str, result: dict[str, Any]) -> list[str]:
    """
    Build the two WhatsApp messages sent after a successful upload.
    """
    rows = f"{result['rows']:,}"
    cols = result['cols']
    detected = result['detected_type'] or 'tabular data'
    report = result.get('cleaning_report')

    # --- Message 1: data summary ---
    lines = [
        f"✅ *{filename}* loaded successfully.\n",
        f"📊 Dataset: {rows} rows × {cols} columns",
        f"🔍 Type detected: {detected}",
    ]

    key_cols = result.get('key_columns', [])
    if key_cols:
        lines.append(f"📋 Columns (first {min(5, len(key_cols))}): {', '.join(key_cols[:5])}")

    if report and report.cleaning_applied:
        lines.append("\nDHIS2 cleaning applied:")
        if report.mojibake_fixed:
            lines.append(f"  • Fixed {len(report.mojibake_fixed)} mojibake column name(s)")
        if report.duplicates_merged:
            lines.append(f"  • Merged {len(report.duplicates_merged)} duplicate column pair(s)")

    msg1 = '\n'.join(lines)

    # --- Message 2: next steps ---
    msg2 = (
        "Your data is ready. You can now ask me to:\n\n"
        "• *Calculate TPR* — \"What is the test positivity rate by ward?\"\n"
        "• *Create a vulnerability map* — \"Show me a risk map\"\n"
        "• *Plan ITN distribution* — \"How many bed nets do we need?\"\n"
        "• *Explore the data* — \"What columns do I have?\" or \"Show me summary statistics\""
    )

    return [msg1, msg2]
