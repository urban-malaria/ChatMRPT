"""
Data Analysis V3 Routes
Handles file uploads and queries for the Data Analysis tab
"""
from __future__ import annotations

import glob
import os
import logging
import time
from pathlib import Path
from flask import Blueprint, request, jsonify, session, current_app
from app.agent.metadata_cache import MetadataCache
from app.tpr.language import TPRLanguageInterface
from app.auth.decorators import require_auth
from app.services.interaction_core import InteractionCore
from app.services.analysis_upload_service import process_analysis_upload
from app.upload.upload_service import UploadService

logger = logging.getLogger(__name__)

# Initialize interaction logger for capturing all user/assistant messages
interaction_core = InteractionCore()

# Create blueprint
data_analysis_v3_bp = Blueprint('data_analysis_v3', __name__)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'txt'}


def _save_and_respond(response_dict, session_id, status_code=200):
    """Save assistant message to SessionMemory, then return JSON response."""
    try:
        from app.services.session_memory import SessionMemory, MessageType
        # Always write to base_session_id so messages stay with the original conversation
        mem_sid = session.get('base_session_id') or session_id
        msg = response_dict.get('message', '') if isinstance(response_dict, dict) else str(response_dict)
        if msg:
            viz = (response_dict.get('visualizations') or []) if isinstance(response_dict, dict) else []
            meta = {'visualizations': viz} if viz else {}
            SessionMemory(mem_sid).add_message(MessageType.ASSISTANT, msg, metadata=meta)
    except Exception as err:
        logger.debug("SessionMemory save (v3 helper) failed: %s", err)
    if status_code != 200:
        return jsonify(response_dict), status_code
    return jsonify(response_dict)


def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def _select_metadata_entry(cache: dict) -> tuple[dict, str] | tuple[None, None]:
    """Pick the most relevant metadata entry from the cache."""
    files = (cache or {}).get('files', {})
    if not files:
        return None, None

    priority = [
        'unified_dataset.csv',
        'data_analysis.csv',
        'raw_data.csv',
        'uploaded_data.csv',
    ]

    for name in priority:
        if name in files:
            return files[name], name

    # Fallback to first available entry
    name, meta = next(iter(files.items()))
    return meta, name


def _build_general_workflow_context(session_id: str) -> dict:
    """Construct workflow context for general (non-TPR) data analysis."""
    context: dict = {
        'workflow': 'data_analysis_v3',
        'stage': 'no_data',
        'valid_options': [],
        'data_loaded': False,
        'session_id': session_id,
    }

    columns: list[str] = []
    rows: int | None = None
    dataset_name: str | None = None

    try:
        cache = MetadataCache.load_cache(session_id) or {}
        metadata, dataset_name = _select_metadata_entry(cache)

        if metadata:
            columns = metadata.get('column_names') or []
            rows = metadata.get('rows') if isinstance(metadata.get('rows'), (int, float)) else None
            profile = metadata.get('profile', {}) or {}
            metrics = profile.get('metrics', {}) or {}

            if not columns:
                columns = metrics.get('column_examples', [])

            dtype_summary = metrics.get('dtype_summary', {})

            context.update({
                'data_loaded': True,
                'data_columns': columns,
                'columns_total': len(columns),
                'data_shape': {
                    'rows': rows,
                    'cols': len(columns),
                },
                'data_types': dtype_summary,
                'dataset_name': dataset_name,
            })

            if metrics.get('numeric_columns'):
                context['numeric_samples'] = metrics['numeric_columns']
            if metrics.get('categorical_columns'):
                context['categorical_samples'] = metrics['categorical_columns']
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(f"[WORKFLOW CONTEXT] Failed to load metadata cache for {session_id}: {exc}")

    session_path = Path('instance/uploads') / session_id

    # If columns look wrong (many Unnamed: entries from wrong header row), try to
    # re-read the Excel using the saved schema's header_row so the agent sees real names.
    unnamed_count = sum(1 for c in columns if str(c).startswith('Unnamed:'))
    if columns and unnamed_count > len(columns) * 0.5:
        try:
            from app.agent.state_manager import DataAnalysisStateManager
            from app.agent.encoding_handler import EncodingHandler

            sm = DataAnalysisStateManager(session_id)
            saved_state = sm.load_state() or {}
            saved_schema = saved_state.get('column_schema')
            header_row = int(saved_schema.get('header_row', 1)) if saved_schema else 1

            for candidate in ['data_analysis.xlsx', 'data_analysis.xls']:
                candidate_path = session_path / candidate
                if candidate_path.exists():
                    sample = EncodingHandler.read_excel_with_encoding(
                        str(candidate_path), header=header_row, nrows=5
                    )
                    real_cols = sample.columns.tolist()
                    if real_cols:
                        columns = real_cols
                        context['data_columns'] = columns
                        context['columns_total'] = len(columns)
                        logger.info(
                            "[WORKFLOW CONTEXT] Re-read Excel with header_row=%d → %d real columns",
                            header_row, len(columns)
                        )
                    break
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(f"[WORKFLOW CONTEXT] Failed to fix Excel header: {exc}")

    if not columns:
        # Fallback: load a tiny sample directly
        for candidate in ['unified_dataset.csv', 'data_analysis.csv', 'raw_data.csv', 'uploaded_data.csv']:
            candidate_path = session_path / candidate
            if candidate_path.exists():
                try:
                    from app.agent.encoding_handler import EncodingHandler

                    sample = EncodingHandler.read_csv_with_encoding(candidate_path, nrows=5)
                    columns = sample.columns.tolist()
                    rows = rows or sample.shape[0]
                    context.update({
                        'data_loaded': True,
                        'data_columns': columns,
                        'columns_total': len(columns),
                        'data_shape': {
                            'rows': rows,
                            'cols': len(columns),
                        },
                        'dataset_name': dataset_name or candidate,
                    })
                    break
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning(f"[WORKFLOW CONTEXT] Failed to sample {candidate_path}: {exc}")

    if session_path.joinpath('unified_dataset.csv').exists():
        context['stage'] = 'post_analysis'
    elif context['data_loaded']:
        context['stage'] = 'data_exploring'

    # Limit columns to avoid overwhelming the prompt
    if context.get('data_columns') and len(context['data_columns']) > 120:
        context['data_columns_preview'] = context['data_columns'][:120]

    # If we have a saved schema, inject a human-readable column mapping so the
    # agent can describe columns with confidence instead of guessing.
    try:
        from app.agent.state_manager import DataAnalysisStateManager

        sm = DataAnalysisStateManager(session_id)
        saved_state = sm.load_state() or {}
        saved_schema = saved_state.get('column_schema')
        if saved_schema:
            _label = {
                'state': 'State',
                'lga': 'LGA (Local Government Area)',
                'ward': 'Ward',
                'facility_name': 'Facility name',
                'facility_level': 'Facility level (Primary/Secondary/Tertiary)',
                'period': 'Reporting period',
                'u5_rdt_tested': 'Under-5 RDT tested (denominator)',
                'u5_rdt_positive': 'Under-5 RDT positive (numerator)',
                'o5_rdt_tested': 'Over-5 RDT tested (denominator)',
                'o5_rdt_positive': 'Over-5 RDT positive (numerator)',
                'pw_rdt_tested': 'Pregnant women RDT tested (denominator)',
                'pw_rdt_positive': 'Pregnant women RDT positive (numerator)',
                'u5_microscopy_tested': 'Under-5 Microscopy tested (denominator)',
                'u5_microscopy_positive': 'Under-5 Microscopy positive (numerator)',
                'o5_microscopy_tested': 'Over-5 Microscopy tested (denominator)',
                'o5_microscopy_positive': 'Over-5 Microscopy positive (numerator)',
                'pw_microscopy_tested': 'Pregnant women Microscopy tested (denominator)',
                'pw_microscopy_positive': 'Pregnant women Microscopy positive (numerator)',
            }
            mapping_lines = [
                f"  {v} → column: \"{saved_schema[k]}\""
                for k, v in _label.items()
                if saved_schema.get(k)
            ]
            if mapping_lines:
                context['column_schema_description'] = (
                    "Known column meanings (from schema inference):\n"
                    + "\n".join(mapping_lines)
                )
    except Exception:
        pass

    return context


class TPRStartError(RuntimeError):
    """Raised by _handle_tpr_start when workflow cannot be started (message is user-facing)."""


def _run_agent_sync(agent, message, workflow_context=None):
    """Run agent.analyze() synchronously in a dedicated event loop."""
    import asyncio as _asyncio
    loop = _asyncio.new_event_loop()
    _asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(agent.analyze(message, workflow_context=workflow_context))
    finally:
        _asyncio.set_event_loop(None)
        loop.close()


def _handle_tpr_active(session_id, message, state_manager, current_state, tpr_language):
    """
    Handle a message when the TPR workflow is already in progress.
    Returns a result dict. Shared by /chat and /stream.
    """
    from app.tpr.workflow_manager import TPRWorkflowHandler
    from app.tpr.data_analyzer import TPRDataAnalyzer
    from app.agent.encoding_handler import EncodingHandler
    from app.agent.state_manager import ConversationStage

    tpr_analyzer = TPRDataAnalyzer()
    saved_state = state_manager.load_state() or {}
    saved_schema = saved_state.get('column_schema')
    if saved_schema:
        tpr_analyzer._schema = saved_schema
        logger.info(
            "[TPR-ACTIVE] Restored column_schema (%d mapped fields)",
            len([v for v in saved_schema.values() if v]),
        )
    else:
        logger.warning("[TPR-ACTIVE] No column_schema in state_manager")

    tpr_handler = TPRWorkflowHandler(session_id, state_manager, tpr_analyzer)

    df = None
    try:
        data_dir = os.path.join('instance', 'uploads', session_id)
        uploaded_csv = os.path.join(data_dir, 'uploaded_data.csv')
        if os.path.exists(uploaded_csv):
            df = EncodingHandler.read_csv_with_encoding(uploaded_csv)
            logger.info("[TPR-ACTIVE] Loaded uploaded_data.csv (%d rows)", df.shape[0])
        else:
            data_files = (
                glob.glob(os.path.join(data_dir, '*.csv')) +
                glob.glob(os.path.join(data_dir, '*.xlsx')) +
                glob.glob(os.path.join(data_dir, '*.xls'))
            )
            if data_files:
                from app.utils.dhis2_cleaner import (
                    _select_raw_upload_file, clean_dhis2_export,
                    get_cleaner_mode, apply_rename_map_to_schema,
                )
                try:
                    latest = _select_raw_upload_file(data_files)
                except FileNotFoundError:
                    latest = None
                if latest:
                    if latest.lower().endswith(('.xlsx', '.xls')):
                        header_row = int(saved_schema.get('header_row', 0)) if saved_schema else 0
                        df = EncodingHandler.read_excel_with_encoding(latest, header=header_row)
                    else:
                        df = EncodingHandler.read_csv_with_encoding(latest)
                    _cleaner_mode = get_cleaner_mode()
                    if _cleaner_mode != 'off':
                        try:
                            df, _cr = clean_dhis2_export(df, mode=_cleaner_mode)
                            if _cr.column_rename_map:
                                saved_schema = apply_rename_map_to_schema(
                                    saved_schema or {}, _cr.column_rename_map
                                )
                                state_manager.update_state({'column_schema': saved_schema})
                                tpr_analyzer._schema = saved_schema
                        except Exception as _cexc:
                            logger.exception("[TPR-ACTIVE] Cleaner failed: %s", _cexc)
        if df is not None:
            tpr_handler.set_data(df)
    except Exception as load_err:
        logger.error("[TPR-ACTIVE] Failed to load dataset: %s", load_err)

    tpr_handler.load_state_from_manager()
    current_stage = state_manager.get_workflow_stage()

    # Build valid_options — TPR_STATE_SELECTION reads actual state names from data
    valid_options = []
    if current_stage == ConversationStage.TPR_STATE_SELECTION:
        if df is not None:
            try:
                state_analysis = tpr_analyzer.analyze_states(df)
                valid_options = list(state_analysis.get('states', {}).keys())
            except Exception:
                pass
        valid_options.extend(['yes', 'continue', 'back', 'exit', 'status'])
    elif current_stage == ConversationStage.TPR_FACILITY_LEVEL:
        valid_options = ['primary', 'secondary', 'tertiary', 'all', 'back', 'exit', 'status']
    elif current_stage == ConversationStage.TPR_AGE_GROUP:
        valid_options = ['u5', 'o5', 'pw', 'all', 'back', 'exit', 'status']
    else:
        valid_options = ['yes', 'continue', 'start', 'back', 'exit']

    lower_message = (message or '').lower().strip()

    # Priority 1: confirmation
    if current_state.get('tpr_awaiting_confirmation'):
        confirmation_keywords = [
            'yes', 'y', 'continue', 'proceed', 'start', 'begin', 'ok', 'okay', 'sure', 'ready'
        ]
        if lower_message in confirmation_keywords or any(
            kw in lower_message.split() for kw in confirmation_keywords
        ):
            logger.info("[TPR-ACTIVE] Confirmation detected")
            return tpr_handler.execute_confirmation()

    # Priority 2: intent classification → selection or question
    intent_result = tpr_language.classify_intent(
        message=message,
        stage=current_stage.name if current_stage else 'unknown',
        valid_options=valid_options,
    )

    if intent_result['intent'] == 'selection' and intent_result['confidence'] >= 0.7:
        logger.info(
            "[TPR-ACTIVE] Selection intent (confidence=%.2f, rationale=%s)",
            intent_result['confidence'],
            intent_result.get('rationale', ''),
        )
        command = tpr_language.extract_command(
            message=message,
            stage=current_stage.name if current_stage else 'unknown',
            valid_options=valid_options,
            context={'session_id': session_id, 'stage': current_stage},
        )
        if command:
            logger.info("[TPR-ACTIVE] Extracted command: '%s'", command)
            return tpr_handler.execute_command(command, current_stage)
        return {
            'success': True,
            'message': (
                f"I understood you're making a selection, but couldn't determine which option. "
                f"Please choose from: {', '.join(valid_options)}"
            ),
            'session_id': session_id,
            'workflow': 'tpr',
            'stage': current_stage.name if current_stage else None,
        }

    # Question → agent
    logger.info("[TPR-ACTIVE] Question detected, routing to agent")
    from app.agent.agent import DataAnalysisAgent
    agent = DataAnalysisAgent(session_id)
    workflow_context = {
        'workflow': 'tpr',
        'stage': current_stage.name if current_stage else None,
        'valid_options': valid_options,
        'selections': state_manager.get_tpr_selections() or {},
        'data_loaded': df is not None,
        'session_id': session_id,
    }
    return _run_agent_sync(agent, message, workflow_context=workflow_context)


def _handle_tpr_start(session_id, message, state_manager, current_state):
    """
    Load data and start the TPR workflow from scratch.
    Returns a result dict. Raises TPRStartError with a user-facing message on failure.
    Shared by /chat and /stream.
    """
    from app.tpr.workflow_manager import TPRWorkflowHandler
    from app.tpr.data_analyzer import TPRDataAnalyzer
    from app.tpr.language import TPRLanguageInterface as _TPRLang
    from app.agent.encoding_handler import EncodingHandler
    from app.agent.state_manager import ConversationStage
    from app.utils.dhis2_cleaner import (
        _select_raw_upload_file, clean_dhis2_export,
        get_cleaner_mode, apply_rename_map_to_schema,
    )

    logger.info("[TPR-START] Starting workflow for session %s", session_id)

    data_dir = os.path.join('instance', 'uploads', session_id)
    data_files = (
        glob.glob(os.path.join(data_dir, '*.csv')) +
        glob.glob(os.path.join(data_dir, '*.xlsx')) +
        glob.glob(os.path.join(data_dir, '*.xls'))
    )
    if not data_files:
        raise TPRStartError(
            'No data found. Please upload your dataset before starting the TPR workflow.'
        )

    tpr_analyzer = TPRDataAnalyzer()
    tpr_language = _TPRLang(session_id)
    try:
        tpr_language.update_from_metadata(current_state)
    except Exception:
        pass

    uploaded_csv = os.path.join(data_dir, 'uploaded_data.csv')
    _saved_schema = (
        current_state.get('column_schema')
        or (state_manager.load_state() or {}).get('column_schema')
        or {}
    )
    _tpr_cols = ('tested_pos', 'u5_pos', 'o5_pos', 'pw_pos', 'total_tested')
    _schema_complete = (
        _saved_schema.get('header_row') is not None
        and any(_saved_schema.get(c) for c in _tpr_cols)
    )

    df = None

    if os.path.exists(uploaded_csv):
        df = EncodingHandler.read_csv_with_encoding(uploaded_csv)
        if _schema_complete:
            tpr_analyzer._schema = _saved_schema
            logger.info("[TPR-START] Using cleaned uploaded_data.csv + saved schema (%d rows)", df.shape[0])
        else:
            try:
                df, schema = tpr_analyzer.infer_schema_from_file(uploaded_csv)
                state_manager.update_state({'column_schema': schema})
                logger.info("[TPR-START] Re-inferred schema from uploaded_data.csv")
            except RuntimeError as exc:
                raise TPRStartError(f'Could not parse your data file: {exc}') from exc
    else:
        try:
            latest = _select_raw_upload_file(data_files)
        except FileNotFoundError:
            raise TPRStartError('No data file found. Please re-upload your dataset.')

        if _schema_complete:
            tpr_analyzer._schema = _saved_schema
            header_row = int(_saved_schema.get('header_row', 0))
            try:
                if latest.lower().endswith(('.xlsx', '.xls')):
                    df = EncodingHandler.read_excel_with_encoding(latest, header=header_row)
                else:
                    df = EncodingHandler.read_csv_with_encoding(latest)
            except Exception as exc:
                logger.warning("[TPR-START] Re-read with saved schema failed (%s), re-inferring", exc)
                _schema_complete = False

        if not _schema_complete:
            try:
                df, schema = tpr_analyzer.infer_schema_from_file(latest)
                state_manager.update_state({'column_schema': schema})
            except RuntimeError as exc:
                raise TPRStartError(f'Could not parse your data file: {exc}') from exc

        _cleaner_mode = get_cleaner_mode()
        if _cleaner_mode != 'off' and df is not None:
            try:
                df, _cr = clean_dhis2_export(df, mode=_cleaner_mode)
                if _cr.column_rename_map:
                    schema = apply_rename_map_to_schema(tpr_analyzer._schema or {}, _cr.column_rename_map)
                    tpr_analyzer._schema = schema
                    state_manager.update_state({'column_schema': schema})
                logger.info("[TPR-START] Applied cleaner to raw re-read (mode=%s)", _cleaner_mode)
            except Exception as exc:
                logger.exception("[TPR-START] Cleaner failed: %s", exc)

    if df is None:
        raise TPRStartError('Could not load data file. Please re-upload your dataset.')

    tpr_handler = TPRWorkflowHandler(session_id, state_manager, tpr_analyzer)
    tpr_handler.set_data(df)
    try:
        tpr_language.update_from_dataframe(df)
    except Exception:
        pass

    state_manager.mark_tpr_workflow_active()
    state_manager.update_workflow_stage(ConversationStage.TPR_STATE_SELECTION)
    logger.info("[TPR-START] Workflow marked active, starting")

    return tpr_handler.start_workflow()


def _wrap_tpr_as_sse(result, session_id, mem_sid, app_obj, interaction_core_obj):
    """Wrap a TPR result dict as a Server-Sent Events Response."""
    import json as _json
    from flask import Response, stream_with_context

    def _generate():
        with app_obj.app_context():
            yield f"data: {_json.dumps({'type': 'status', 'status': 'started'})}\n\n"
            yield f"data: {_json.dumps({'type': 'result', 'data': result})}\n\n"
            assistant_text = result.get('message', '') if isinstance(result, dict) else str(result)
            try:
                from app.services.session_memory import SessionMemory, MessageType
                SessionMemory(mem_sid).add_message(MessageType.ASSISTANT, assistant_text)
            except Exception:
                pass
            try:
                interaction_core_obj.log_message(
                    session_id=session_id, sender='assistant',
                    content=assistant_text, intent=None,
                    entities={
                        'endpoint': '/api/v1/data-analysis/chat/stream',
                        'workflow': result.get('workflow', 'tpr') if isinstance(result, dict) else 'tpr',
                        'success': result.get('success', True) if isinstance(result, dict) else False,
                    }
                )
            except Exception:
                pass
            yield "data: [DONE]\n\n"

    resp = Response(stream_with_context(_generate()), mimetype='text/event-stream')
    resp.headers['Cache-Control'] = 'no-cache'
    resp.headers['Connection'] = 'keep-alive'
    resp.headers['X-Accel-Buffering'] = 'no'
    return resp


@data_analysis_v3_bp.route('/api/data-analysis/upload', methods=['POST'])
@require_auth
def upload_for_analysis():
    """
    Handle file uploads from the Data Analysis tab.
    This is specifically for general data exploration, not malaria risk analysis.
    """
    try:
        previous_session_id = session.get('session_id')
        base_session_id = session.get('base_session_id') or previous_session_id

        # ALWAYS generate a new session ID for each upload to prevent session reuse
        # This fixes the concurrent user data bleed issue
        import uuid
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
        if base_session_id:
            session['base_session_id'] = base_session_id
        else:
            base_session_id = session_id
            session['base_session_id'] = base_session_id

        # Log for debugging
        logger.info(f"📊 Generated new session ID for upload: {session_id}")

        # Store the upload session_id in the base session's SessionMemory
        # (via key_entities which survives _save_memory rewrites) so resume
        # can find the uploaded files in the child session folder.
        if base_session_id and base_session_id != session_id:
            try:
                from app.services.session_memory import SessionMemory as _SM
                _base_mem = _SM(base_session_id)
                _base_mem.key_entities['upload_session_id'] = session_id
                _base_mem._save_memory()
                logger.info(f"📊 Stored upload_session_id={session_id} in base session {base_session_id}")
            except Exception as _link_err:
                logger.debug(f"Failed to link upload session to base: {_link_err}")

        # Check for file
        if 'file' not in request.files:
            return jsonify({
                'status': 'error',
                'message': 'No file provided'
            }), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({
                'status': 'error',
                'message': 'No file selected'
            }), 400
        
        if not allowed_file(file.filename):
            return jsonify({
                'status': 'error',
                'message': f'Invalid file type. Allowed types: {", ".join(ALLOWED_EXTENSIONS)}'
            }), 400
        
        upload_root = current_app.config.get('UPLOAD_FOLDER', 'instance/uploads')
        upload_result = process_analysis_upload(
            session_id=session_id,
            file_obj=file,
            original_filename=file.filename,
            upload_root=upload_root,
        )

        filename = upload_result.original_filename
        standard_path = str(upload_result.standard_path)
        metadata = upload_result.metadata or {}

        logger.info(f"📊 Data Analysis file uploaded: {filename} for session {session_id}")
        if metadata.get('is_sampled'):
            logger.info(f"📊 Large file detected ({metadata.get('file_size_mb')}MB), using sampling for metadata")
        
        # Store in session with workflow context
        session['has_data_analysis_file'] = True
        session['data_analysis_filename'] = filename
        session['workflow_source'] = 'data_analysis_v3'  # Track where workflow originated
        session['workflow_stage'] = 'uploaded'  # Track current stage

        # CRITICAL: Set flags for routing logic to recognize Data Analysis V3 mode
        session['use_data_analysis_v3'] = True
        session['csv_loaded'] = True
        session['data_analysis_active'] = True
        session['active_tab'] = 'data-analysis'

        session.modified = True
        logger.info(f"✓ Data Analysis V3 mode activated for session {session_id}")
        
        # Sync to other instances for multi-instance support
        try:
            from app.services.instance_sync import sync_session_after_upload
            sync_session_after_upload(session_id)
            logger.info(f"🔄 Initiated sync for session {session_id}")
        except Exception as e:
            logger.warning(f"Could not sync to other instances: {e}")

        if base_session_id and base_session_id != session_id:
            try:
                upload_service = UploadService(current_app.config.get('UPLOAD_FOLDER', 'instance/uploads'))
                upload_service.mirror_artifacts(
                    source_session_id=session_id,
                    target_session_id=base_session_id,
                )
                logger.info(
                    f"🗂️ Mirrored upload artefacts from {session_id} to base session {base_session_id}"
                )
            except Exception as mirror_exc:
                logger.warning(
                    f"Could not mirror artefacts to base session {base_session_id}: {mirror_exc}"
                )

        # Prepare response with metadata info
        response_data = {
            'status': 'success',
            'message': f'File "{filename}" uploaded successfully',
            'session_id': session_id,
            'filename': filename,
            'filepath': standard_path,
            'file_size': upload_result.file_size,
            'metadata': {
                'rows': metadata.get('rows', 'Unknown'),
                'columns': metadata.get('columns', 'Unknown'),
                'is_sampled': metadata.get('is_sampled', False),
                'file_size_mb': metadata.get('file_size_mb', 0)
            }
        }
        
        # Add estimation note if applicable
        if metadata.get('rows_estimated'):
            response_data['metadata']['rows_note'] = 'Estimated from file size'
        
        # Add instance identifier for debugging
        import socket
        response_data['instance'] = socket.gethostname()
        
        response = jsonify(response_data)
        
        # Set cookie for session affinity
        response.set_cookie(
            'ChatMRPT-Session',
            value=session_id,
            max_age=3600,  # 1 hour
            httponly=True,
            samesite='Lax',
            path='/'
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error in data analysis upload: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Upload failed: {str(e)}'
        }), 500


@data_analysis_v3_bp.route('/api/data-analysis/status', methods=['GET'])
@require_auth
def get_upload_status():
    """Check if data has been uploaded for analysis."""
    session_id = session.get('session_id')
    has_file = session.get('has_data_analysis_file', False)
    filename = session.get('data_analysis_filename', None)
    
    return jsonify({
        'status': 'success',
        'has_file': has_file,
        'filename': filename,
        'session_id': session_id
    })


@data_analysis_v3_bp.route('/api/data-analysis/clear-mode', methods=['POST'])
@require_auth
def clear_data_analysis_mode():
    """Clear Data Analysis V3 mode when switching tabs."""
    session['has_data_analysis_file'] = False
    session['use_data_analysis_v3'] = False
    session['active_tab'] = 'standard-upload'
    session.modified = True
    
    logger.info(f"📊 Data Analysis V3 mode cleared for session {session.get('session_id')}")
    
    return jsonify({
        'status': 'success',
        'message': 'Data Analysis mode cleared'
    })


@data_analysis_v3_bp.route('/api/data-analysis/activate-mode', methods=['POST'])
@require_auth
def activate_data_analysis_mode():
    """Activate Data Analysis V3 mode when tab is selected with data."""
    if session.get('has_data_analysis_file', False):
        session['use_data_analysis_v3'] = True
        session['active_tab'] = 'data-analysis'
        session.modified = True
        
        logger.info(f"📊 Data Analysis V3 mode activated for session {session.get('session_id')}")
        
        return jsonify({
            'status': 'success',
            'message': 'Data Analysis mode activated'
        })
    else:
        return jsonify({
            'status': 'info',
            'message': 'No data file uploaded yet'
        })


@data_analysis_v3_bp.route('/images/plotly_figures/pickle/<path:filename>')
@require_auth
def serve_pickle_visualization(filename):
    """
    Serve pickle visualization files or convert to HTML on-the-fly.
    This fixes the 502 error when frontend requests pickle files.
    """
    import pickle
    import os
    import uuid

    try:
        # Get session ID from session or cookie
        session_id = session.get('session_id')
        if not session_id:
            # Try to extract from filename pattern if it includes session ID
            # Otherwise return 404
            logger.warning(f"No session ID for pickle request: {filename}")
            return jsonify({'error': 'Session not found'}), 404

        # Try multiple possible locations for pickle files
        possible_paths = [
            f"instance/uploads/{session_id}/visualizations/{filename}",
            f"instance/uploads/{session_id}/{filename}",
            f"app/static/visualizations/{filename}",
        ]

        pickle_path = None
        for path in possible_paths:
            if os.path.exists(path):
                pickle_path = path
                break

        if not pickle_path:
            logger.error(f"Pickle file not found: {filename} for session {session_id}")
            return jsonify({'error': 'Visualization not found'}), 404

        # Load the pickle file
        with open(pickle_path, 'rb') as f:
            fig = pickle.load(f)

        # Convert to HTML
        html_content = fig.to_html(include_plotlyjs=True)

        # Return HTML directly
        from flask import Response
        return Response(html_content, mimetype='text/html')

    except Exception as e:
        logger.error(f"Error serving pickle visualization {filename}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@data_analysis_v3_bp.route('/static/visualizations/<path:filename>')
@require_auth
def serve_static_visualization(filename):
    """
    Serve HTML visualization files from the static directory.
    This ensures proper serving of Data Analysis V3 generated visualizations.
    """
    try:
        from flask import send_from_directory
        import os

        # Serve from the static visualizations directory
        viz_dir = os.path.join(current_app.root_path, 'static', 'visualizations')

        if not os.path.exists(os.path.join(viz_dir, filename)):
            logger.warning(f"Visualization file not found: {filename}")
            return jsonify({'error': 'Visualization not found'}), 404

        # Serve the HTML file
        return send_from_directory(viz_dir, filename, mimetype='text/html')

    except Exception as e:
        logger.error(f"Error serving static visualization {filename}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@data_analysis_v3_bp.route('/api/v1/data-analysis/chat', methods=['POST'])
@require_auth
def data_analysis_chat():
    """
    Handle chat messages for Data Analysis V3.
    Routes to: TPR active workflow → TPR start → general agent.
    """
    try:
        data = request.get_json() or {}
        message = data.get('message', '')
        session_id = data.get('session_id') or session.get('session_id')

        existing_session_id = session.get('session_id')
        if session_id and existing_session_id and existing_session_id != session_id:
            logger.info(
                "[DATA-ANALYSIS] Realigning session context from %s to client session %s",
                existing_session_id,
                session_id,
            )
            session['session_id'] = session_id
            session['base_session_id'] = session.get('base_session_id') or session_id
            session.modified = True
        elif session_id and not existing_session_id:
            session['session_id'] = session_id
            session['base_session_id'] = session.get('base_session_id') or session_id
            session.modified = True

        if not session_id:
            return jsonify({
                'success': False,
                'error': 'No session ID provided'
            }), 400

        logger.info("[CHAT] Session %s: %s", session_id, message[:80])

        request_start_time = time.time()
        interaction_core.log_message(
            session_id=session_id, sender='user', content=message, intent=None,
            entities={
                'message_length': len(message),
                'timestamp': request_start_time,
                'endpoint': '/api/v1/data-analysis/chat',
                'workflow': 'data_analysis_v3',
            }
        )

        mem_sid = session.get('base_session_id') or session_id
        try:
            from app.services.session_memory import SessionMemory, MessageType
            SessionMemory(mem_sid).add_message(MessageType.USER, message)
        except Exception:
            pass

        from app.agent.state_manager import DataAnalysisStateManager
        state_manager = DataAnalysisStateManager(session_id)
        current_state = state_manager.get_state() or {}

        tpr_language = TPRLanguageInterface(session_id)
        try:
            tpr_language.update_from_metadata(current_state)
        except Exception:
            pass

        if current_state.get('workflow_transitioned'):
            logger.info("Workflow transitioned for session %s — staying in V3 agent mode", session_id)

        lower_message = (message or '').lower().strip()
        is_tpr_active = state_manager.is_tpr_workflow_active()

        if is_tpr_active:
            logger.info("[CHAT] TPR active for session %s", session_id)
            response = _handle_tpr_active(session_id, message, state_manager, current_state, tpr_language)
            response_time = time.time() - request_start_time
            assistant_message = response.get('message', '') if isinstance(response, dict) else str(response)
            interaction_core.log_message(
                session_id=session_id, sender='assistant', content=assistant_message,
                intent=response.get('workflow') if isinstance(response, dict) else None,
                entities={
                    'response_length': len(assistant_message),
                    'response_time_seconds': response_time,
                    'endpoint': '/api/v1/data-analysis/chat',
                    'workflow': 'tpr',
                    'stage': response.get('stage') if isinstance(response, dict) else None,
                    'visualizations_count': len(response.get('visualizations') or []) if isinstance(response, dict) else 0,
                    'status': response.get('success', True) if isinstance(response, dict) else True,
                }
            )
            return _save_and_respond(response, session_id)

        start_triggers = ['start tpr', 'start the tpr', 'tpr workflow', 'run tpr']
        if any(t in lower_message for t in start_triggers):
            try:
                response = _handle_tpr_start(session_id, message, state_manager, current_state)
            except TPRStartError as e:
                response = {'success': False, 'message': str(e), 'session_id': session_id}
            return _save_and_respond(response, session_id)

        from app.agent.agent import DataAnalysisAgent
        agent = DataAnalysisAgent(session_id)
        workflow_context = _build_general_workflow_context(session_id)
        logger.info(
            "[AGENT CONTEXT] Session %s → stage=%s columns=%d",
            session_id,
            workflow_context.get('stage'),
            len(workflow_context.get('data_columns') or [])
        )
        result = _run_agent_sync(agent, message, workflow_context=workflow_context)

        response_time = time.time() - request_start_time
        assistant_message = result.get('message', '') if isinstance(result, dict) else str(result)
        interaction_core.log_message(
            session_id=session_id, sender='assistant', content=assistant_message,
            intent='agent_query',
            entities={
                'response_length': len(assistant_message),
                'response_time_seconds': response_time,
                'endpoint': '/api/v1/data-analysis/chat',
                'workflow': 'data_analysis_v3_agent',
                'visualizations_count': len(result.get('visualizations') or []) if isinstance(result, dict) else 0,
                'status': result.get('success', True) if isinstance(result, dict) else True,
            }
        )
        logger.info("✅ Agent response logged for session %s (%.2fs)", session_id, response_time)
        return _save_and_respond(result, session_id)

    except Exception as e:
        logger.error("Error in data analysis chat: %s", str(e), exc_info=True)
        import traceback
        error_message = f"Error: {str(e)}"
        try:
            interaction_core.log_message(
                session_id=session_id if 'session_id' in locals() else 'unknown',
                sender='assistant', content=error_message, intent='error',
                entities={
                    'error_type': type(e).__name__,
                    'endpoint': '/api/v1/data-analysis/chat',
                    'workflow': 'error',
                    'traceback': traceback.format_exc(),
                }
            )
        except Exception as log_err:
            logger.error("Failed to log error: %s", log_err)
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc(),
        }), 500


@data_analysis_v3_bp.route('/api/v1/data-analysis/chat/stream', methods=['POST'])
@require_auth
def data_analysis_chat_stream():
    """
    SSE streaming endpoint for data analysis chat.
    TPR paths run synchronously and are SSE-wrapped via _wrap_tpr_as_sse().
    General queries stream token-by-token via agent.analyze_stream().
    """
    import json as _json
    from flask import Response, stream_with_context

    data = request.get_json() or {}
    message = data.get('message', '')
    session_id = data.get('session_id') or session.get('session_id')

    if not session_id:
        def _no_session():
            yield f"data: {_json.dumps({'type': 'error', 'error': 'No session ID provided'})}\n\n"
            yield "data: [DONE]\n\n"
        return Response(stream_with_context(_no_session()), mimetype='text/event-stream')

    existing_session_id = session.get('session_id')
    if session_id and existing_session_id and existing_session_id != session_id:
        session['session_id'] = session_id
        session['base_session_id'] = session.get('base_session_id') or session_id
        session.modified = True
    elif session_id and not existing_session_id:
        session['session_id'] = session_id
        session['base_session_id'] = session.get('base_session_id') or session_id
        session.modified = True

    interaction_core.log_message(
        session_id=session_id, sender='user', content=message, intent=None,
        entities={
            'message_length': len(message),
            'timestamp': time.time(),
            'endpoint': '/api/v1/data-analysis/chat/stream',
            'workflow': 'data_analysis_v3',
        }
    )

    mem_sid = session.get('base_session_id') or session_id
    try:
        from app.services.session_memory import SessionMemory, MessageType
        SessionMemory(mem_sid).add_message(MessageType.USER, message)
    except Exception:
        pass

    from app.agent.state_manager import DataAnalysisStateManager
    state_manager = DataAnalysisStateManager(session_id)
    current_state = state_manager.get_state() or {}
    lower_message = (message or '').lower().strip()
    is_tpr_active = state_manager.is_tpr_workflow_active()
    app_obj = current_app._get_current_object()

    if current_state.get('workflow_transitioned'):
        logger.info("Workflow transitioned for session %s — staying in V3 agent mode", session_id)

    if is_tpr_active:
        tpr_language = TPRLanguageInterface(session_id)
        try:
            tpr_language.update_from_metadata(current_state)
        except Exception:
            pass
        try:
            result = _handle_tpr_active(session_id, message, state_manager, current_state, tpr_language)
        except Exception as e:
            logger.exception("[STREAM-TPR-ACTIVE] Error for session %s: %s", session_id, e)
            result = {'success': False, 'message': str(e), 'session_id': session_id}
        return _wrap_tpr_as_sse(result, session_id, mem_sid, app_obj, interaction_core)

    start_triggers = ['start tpr', 'start the tpr', 'tpr workflow', 'run tpr']
    if any(t in lower_message for t in start_triggers):
        try:
            result = _handle_tpr_start(session_id, message, state_manager, current_state)
        except TPRStartError as e:
            result = {'success': False, 'message': str(e), 'session_id': session_id}
        except Exception as e:
            logger.exception("[STREAM-TPR-START] Unexpected error for session %s: %s", session_id, e)
            result = {'success': False, 'message': 'Failed to start TPR workflow.', 'session_id': session_id}
        return _wrap_tpr_as_sse(result, session_id, mem_sid, app_obj, interaction_core)

    # Agent streaming path — analyze_stream() is inherently different from sync analyze()
    workflow_context = _build_general_workflow_context(session_id)
    captured_session_id = session_id
    captured_mem_sid = mem_sid

    def generate():
        with app_obj.app_context():
            yield f"data: {_json.dumps({'type': 'status', 'status': 'started'})}\n\n"
            try:
                from app.agent.agent import DataAnalysisAgent
                from app.services.session_memory import SessionMemory, MessageType
                agent = DataAnalysisAgent(captured_session_id)
                request_start = time.time()
                for event in agent.analyze_stream(message, workflow_context=workflow_context):
                    yield f"data: {_json.dumps(event)}\n\n"
                    if event.get('type') == 'result':
                        response_time = time.time() - request_start
                        assistant_text = event.get('data', {}).get('message', '')
                        try:
                            SessionMemory(captured_mem_sid).add_message(MessageType.ASSISTANT, assistant_text)
                        except Exception:
                            pass
                        try:
                            interaction_core.log_message(
                                session_id=captured_session_id, sender='assistant',
                                content=assistant_text, intent='agent_query',
                                entities={
                                    'response_time_seconds': response_time,
                                    'endpoint': '/api/v1/data-analysis/chat/stream',
                                    'workflow': 'data_analysis_v3_agent',
                                    'visualizations_count': len(
                                        event.get('data', {}).get('visualizations') or []
                                    ),
                                }
                            )
                        except Exception:
                            pass
            except Exception as e:
                logger.exception("[STREAM] Agent error for session %s: %s", captured_session_id, e)
                try:
                    interaction_core.log_message(
                        session_id=captured_session_id, sender='assistant',
                        content=f'Error: {e}', intent='error',
                        entities={
                            'error_type': type(e).__name__,
                            'endpoint': '/api/v1/data-analysis/chat/stream',
                        }
                    )
                except Exception:
                    pass
                yield f"data: {_json.dumps({'type': 'error', 'error': str(e)})}\n\n"
            yield "data: [DONE]\n\n"

    resp = Response(stream_with_context(generate()), mimetype='text/event-stream')
    resp.headers['Cache-Control'] = 'no-cache'
    resp.headers['Connection'] = 'keep-alive'
    resp.headers['X-Accel-Buffering'] = 'no'
    return resp
