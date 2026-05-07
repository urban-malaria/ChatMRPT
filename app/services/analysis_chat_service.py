"""Shared Data Analysis V3 chat orchestration.

This service owns the routing decision between active TPR workflow, TPR start,
and the general DataAnalysisAgent path. Flask routes remain responsible for
HTTP request/response formatting, browser session flags, and interaction logs.
"""

from __future__ import annotations

import glob
import logging
import os
from pathlib import Path
from typing import Any, Iterator

from app.agent.metadata_cache import MetadataCache
from app.tpr.language import TPRLanguageInterface

logger = logging.getLogger(__name__)


class TPRStartError(RuntimeError):
    """Raised when the TPR workflow cannot start; message is user-facing."""


def _select_metadata_entry(cache: dict) -> tuple[dict, str] | tuple[None, None]:
    """Pick the most relevant metadata entry from the cache."""
    files = (cache or {}).get("files", {})
    if not files:
        return None, None

    priority = [
        "unified_dataset.csv",
        "raw_data.csv",
        "tpr_results.csv",
        "data_analysis.csv",
        "uploaded_data.csv",
    ]

    for name in priority:
        if name in files:
            return files[name], name

    name, meta = next(iter(files.items()))
    return meta, name


def build_general_workflow_context(session_id: str) -> dict:
    """Construct workflow context for general non-TPR data analysis."""
    context: dict = {
        "workflow": "data_analysis_v3",
        "stage": "no_data",
        "valid_options": [],
        "data_loaded": False,
        "session_id": session_id,
    }

    columns: list[str] = []
    rows: int | None = None
    dataset_name: str | None = None

    try:
        cache = MetadataCache.load_cache(session_id) or {}
        metadata, dataset_name = _select_metadata_entry(cache)

        if metadata:
            columns = metadata.get("column_names") or []
            rows = metadata.get("rows") if isinstance(metadata.get("rows"), (int, float)) else None
            profile = metadata.get("profile", {}) or {}
            metrics = profile.get("metrics", {}) or {}

            if not columns:
                columns = metrics.get("column_examples", [])

            dtype_summary = metrics.get("dtype_summary", {})

            context.update({
                "data_loaded": True,
                "data_columns": columns,
                "columns_total": len(columns),
                "data_shape": {
                    "rows": rows,
                    "cols": len(columns),
                },
                "data_types": dtype_summary,
                "dataset_name": dataset_name,
            })

            if metrics.get("numeric_columns"):
                context["numeric_samples"] = metrics["numeric_columns"]
            if metrics.get("categorical_columns"):
                context["categorical_samples"] = metrics["categorical_columns"]
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("[WORKFLOW CONTEXT] Failed to load metadata cache for %s: %s", session_id, exc)

    session_path = Path("instance/uploads") / session_id

    unnamed_count = sum(1 for c in columns if str(c).startswith("Unnamed:"))
    if columns and unnamed_count > len(columns) * 0.5:
        try:
            from app.agent.encoding_handler import EncodingHandler
            from app.agent.state_manager import DataAnalysisStateManager

            sm = DataAnalysisStateManager(session_id)
            saved_state = sm.load_state() or {}
            saved_schema = saved_state.get("column_schema")
            header_row = int(saved_schema.get("header_row", 1)) if saved_schema else 1

            for candidate in ["data_analysis.xlsx", "data_analysis.xls"]:
                candidate_path = session_path / candidate
                if candidate_path.exists():
                    sample = EncodingHandler.read_excel_with_encoding(
                        str(candidate_path), header=header_row, nrows=5
                    )
                    real_cols = sample.columns.tolist()
                    if real_cols:
                        columns = real_cols
                        context["data_columns"] = columns
                        context["columns_total"] = len(columns)
                        logger.info(
                            "[WORKFLOW CONTEXT] Re-read Excel with header_row=%d -> %d real columns",
                            header_row,
                            len(columns),
                        )
                    break
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("[WORKFLOW CONTEXT] Failed to fix Excel header: %s", exc)

    if not columns:
        for candidate in [
            "unified_dataset.csv",
            "raw_data.csv",
            "tpr_results.csv",
            "data_analysis.csv",
            "uploaded_data.csv",
        ]:
            candidate_path = session_path / candidate
            if candidate_path.exists():
                try:
                    from app.agent.encoding_handler import EncodingHandler

                    sample = EncodingHandler.read_csv_with_encoding(candidate_path, nrows=5)
                    columns = sample.columns.tolist()
                    rows = rows or sample.shape[0]
                    context.update({
                        "data_loaded": True,
                        "data_columns": columns,
                        "columns_total": len(columns),
                        "data_shape": {
                            "rows": rows,
                            "cols": len(columns),
                        },
                        "dataset_name": dataset_name or candidate,
                    })
                    break
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("[WORKFLOW CONTEXT] Failed to sample %s: %s", candidate_path, exc)

    if session_path.joinpath("unified_dataset.csv").exists():
        context["stage"] = "post_analysis"
    elif context["data_loaded"]:
        context["stage"] = "data_exploring"

    if context.get("data_columns") and len(context["data_columns"]) > 120:
        context["data_columns_preview"] = context["data_columns"][:120]

    try:
        from app.agent.state_manager import DataAnalysisStateManager

        sm = DataAnalysisStateManager(session_id)
        saved_state = sm.load_state() or {}
        saved_schema = saved_state.get("column_schema")
        if saved_schema:
            label = {
                "state": "State",
                "lga": "LGA (Local Government Area)",
                "ward": "Ward",
                "facility_name": "Facility name",
                "facility_level": "Facility level (Primary/Secondary/Tertiary)",
                "period": "Reporting period",
                "u5_rdt_tested": "Under-5 RDT tested (denominator)",
                "u5_rdt_positive": "Under-5 RDT positive (numerator)",
                "o5_rdt_tested": "Over-5 RDT tested (denominator)",
                "o5_rdt_positive": "Over-5 RDT positive (numerator)",
                "pw_rdt_tested": "Pregnant women RDT tested (denominator)",
                "pw_rdt_positive": "Pregnant women RDT positive (numerator)",
                "u5_microscopy_tested": "Under-5 Microscopy tested (denominator)",
                "u5_microscopy_positive": "Under-5 Microscopy positive (numerator)",
                "o5_microscopy_tested": "Over-5 Microscopy tested (denominator)",
                "o5_microscopy_positive": "Over-5 Microscopy positive (numerator)",
                "pw_microscopy_tested": "Pregnant women Microscopy tested (denominator)",
                "pw_microscopy_positive": "Pregnant women Microscopy positive (numerator)",
            }
            mapping_lines = [
                f'  {v} -> column: "{saved_schema[k]}"'
                for k, v in label.items()
                if saved_schema.get(k)
            ]
            if mapping_lines:
                context["column_schema_description"] = (
                    "Known column meanings (from schema inference):\n"
                    + "\n".join(mapping_lines)
                )
    except Exception:
        pass

    return context


def _run_agent_sync(agent, message: str, workflow_context: dict | None = None):
    """Run agent.analyze() synchronously in a dedicated event loop."""
    import asyncio as _asyncio

    loop = _asyncio.new_event_loop()
    _asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(agent.analyze(message, workflow_context=workflow_context))
    finally:
        _asyncio.set_event_loop(None)
        loop.close()


def handle_tpr_active(session_id, message, state_manager, current_state, tpr_language):
    """Handle a message when the TPR workflow is already in progress."""
    from app.agent.encoding_handler import EncodingHandler
    from app.agent.state_manager import ConversationStage
    from app.tpr.data_analyzer import TPRDataAnalyzer
    from app.tpr.workflow_manager import TPRWorkflowHandler

    tpr_analyzer = TPRDataAnalyzer()
    saved_state = state_manager.load_state() or {}
    saved_schema = saved_state.get("column_schema")
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
        data_dir = os.path.join("instance", "uploads", session_id)
        uploaded_csv = os.path.join(data_dir, "uploaded_data.csv")
        if os.path.exists(uploaded_csv):
            df = EncodingHandler.read_csv_with_encoding(uploaded_csv)
            logger.info("[TPR-ACTIVE] Loaded uploaded_data.csv (%d rows)", df.shape[0])
        else:
            data_files = (
                glob.glob(os.path.join(data_dir, "*.csv")) +
                glob.glob(os.path.join(data_dir, "*.xlsx")) +
                glob.glob(os.path.join(data_dir, "*.xls"))
            )
            if data_files:
                from app.utils.dhis2_cleaner import (
                    _select_raw_upload_file,
                    apply_rename_map_to_schema,
                    clean_dhis2_export,
                    get_cleaner_mode,
                )

                try:
                    latest = _select_raw_upload_file(data_files)
                except FileNotFoundError:
                    latest = None
                if latest:
                    if latest.lower().endswith((".xlsx", ".xls")):
                        header_row = int(saved_schema.get("header_row", 0)) if saved_schema else 0
                        df = EncodingHandler.read_excel_with_encoding(latest, header=header_row)
                    else:
                        df = EncodingHandler.read_csv_with_encoding(latest)
                    cleaner_mode = get_cleaner_mode()
                    if cleaner_mode != "off":
                        try:
                            df, cleaning_report = clean_dhis2_export(df, mode=cleaner_mode)
                            if cleaning_report.column_rename_map:
                                saved_schema = apply_rename_map_to_schema(
                                    saved_schema or {}, cleaning_report.column_rename_map
                                )
                                state_manager.update_state({"column_schema": saved_schema})
                                tpr_analyzer._schema = saved_schema
                        except Exception as exc:
                            logger.exception("[TPR-ACTIVE] Cleaner failed: %s", exc)
        if df is not None:
            tpr_handler.set_data(df)
    except Exception as exc:
        logger.error("[TPR-ACTIVE] Failed to load dataset: %s", exc)

    tpr_handler.load_state_from_manager()
    current_stage = state_manager.get_workflow_stage()

    valid_options = []
    if current_stage == ConversationStage.TPR_STATE_SELECTION:
        if df is not None:
            try:
                state_analysis = tpr_analyzer.analyze_states(df)
                valid_options = list(state_analysis.get("states", {}).keys())
            except Exception:
                pass
        valid_options.extend(["yes", "continue", "back", "exit", "status"])
    elif current_stage == ConversationStage.TPR_FACILITY_LEVEL:
        valid_options = ["primary", "secondary", "tertiary", "all", "back", "exit", "status"]
    elif current_stage == ConversationStage.TPR_AGE_GROUP:
        valid_options = ["u5", "o5", "pw", "all", "back", "exit", "status"]
    else:
        valid_options = ["yes", "continue", "start", "back", "exit"]

    lower_message = (message or "").lower().strip()

    if current_state.get("tpr_awaiting_confirmation"):
        confirmation_keywords = [
            "yes", "y", "continue", "proceed", "start", "begin", "ok", "okay", "sure", "ready"
        ]
        if lower_message in confirmation_keywords or any(
            kw in lower_message.split() for kw in confirmation_keywords
        ):
            logger.info("[TPR-ACTIVE] Confirmation detected")
            return tpr_handler.execute_confirmation()

    intent_result = tpr_language.classify_intent(
        message=message,
        stage=current_stage.name if current_stage else "unknown",
        valid_options=valid_options,
    )

    if intent_result["intent"] == "selection" and intent_result["confidence"] >= 0.7:
        logger.info(
            "[TPR-ACTIVE] Selection intent (confidence=%.2f, rationale=%s)",
            intent_result["confidence"],
            intent_result.get("rationale", ""),
        )
        command = tpr_language.extract_command(
            message=message,
            stage=current_stage.name if current_stage else "unknown",
            valid_options=valid_options,
            context={"session_id": session_id, "stage": current_stage},
        )
        if command:
            logger.info("[TPR-ACTIVE] Extracted command: '%s'", command)
            return tpr_handler.execute_command(command, current_stage)
        return {
            "success": True,
            "message": (
                "I understood you're making a selection, but couldn't determine which option. "
                f"Please choose from: {', '.join(valid_options)}"
            ),
            "session_id": session_id,
            "workflow": "tpr",
            "stage": current_stage.name if current_stage else None,
        }

    logger.info("[TPR-ACTIVE] Question detected, routing to agent")
    from app.agent.agent import DataAnalysisAgent

    agent = DataAnalysisAgent(session_id)
    workflow_context = {
        "workflow": "tpr",
        "stage": current_stage.name if current_stage else None,
        "valid_options": valid_options,
        "selections": state_manager.get_tpr_selections() or {},
        "data_loaded": df is not None,
        "session_id": session_id,
    }
    return _run_agent_sync(agent, message, workflow_context=workflow_context)


def handle_tpr_start(session_id, message, state_manager, current_state):
    """Load data and start the TPR workflow from scratch."""
    from app.agent.encoding_handler import EncodingHandler
    from app.agent.state_manager import ConversationStage
    from app.tpr.data_analyzer import TPRDataAnalyzer
    from app.tpr.language import TPRLanguageInterface as _TPRLang
    from app.tpr.workflow_manager import TPRWorkflowHandler
    from app.utils.dhis2_cleaner import (
        _select_raw_upload_file,
        apply_rename_map_to_schema,
        clean_dhis2_export,
        get_cleaner_mode,
    )

    logger.info("[TPR-START] Starting workflow for session %s", session_id)

    data_dir = os.path.join("instance", "uploads", session_id)
    data_files = (
        glob.glob(os.path.join(data_dir, "*.csv")) +
        glob.glob(os.path.join(data_dir, "*.xlsx")) +
        glob.glob(os.path.join(data_dir, "*.xls"))
    )
    if not data_files:
        raise TPRStartError("No data found. Please upload your dataset before starting the TPR workflow.")

    tpr_analyzer = TPRDataAnalyzer()
    tpr_language = _TPRLang(session_id)
    try:
        tpr_language.update_from_metadata(current_state)
    except Exception:
        pass

    uploaded_csv = os.path.join(data_dir, "uploaded_data.csv")
    saved_schema = (
        current_state.get("column_schema")
        or (state_manager.load_state() or {}).get("column_schema")
        or {}
    )
    tpr_cols = ("tested_pos", "u5_pos", "o5_pos", "pw_pos", "total_tested")
    schema_complete = (
        saved_schema.get("header_row") is not None
        and any(saved_schema.get(c) for c in tpr_cols)
    )

    df = None

    if os.path.exists(uploaded_csv):
        df = EncodingHandler.read_csv_with_encoding(uploaded_csv)
        if schema_complete:
            tpr_analyzer._schema = saved_schema
            logger.info("[TPR-START] Using cleaned uploaded_data.csv + saved schema (%d rows)", df.shape[0])
        else:
            try:
                df, schema = tpr_analyzer.infer_schema_from_file(uploaded_csv)
                state_manager.update_state({"column_schema": schema})
                logger.info("[TPR-START] Re-inferred schema from uploaded_data.csv")
            except RuntimeError as exc:
                raise TPRStartError(f"Could not parse your data file: {exc}") from exc
    else:
        try:
            latest = _select_raw_upload_file(data_files)
        except FileNotFoundError:
            raise TPRStartError("No data file found. Please re-upload your dataset.")

        if schema_complete:
            tpr_analyzer._schema = saved_schema
            header_row = int(saved_schema.get("header_row", 0))
            try:
                if latest.lower().endswith((".xlsx", ".xls")):
                    df = EncodingHandler.read_excel_with_encoding(latest, header=header_row)
                else:
                    df = EncodingHandler.read_csv_with_encoding(latest)
            except Exception as exc:
                logger.warning("[TPR-START] Re-read with saved schema failed (%s), re-inferring", exc)
                schema_complete = False

        if not schema_complete:
            try:
                df, schema = tpr_analyzer.infer_schema_from_file(latest)
                state_manager.update_state({"column_schema": schema})
            except RuntimeError as exc:
                raise TPRStartError(f"Could not parse your data file: {exc}") from exc

        cleaner_mode = get_cleaner_mode()
        if cleaner_mode != "off" and df is not None:
            try:
                df, cleaning_report = clean_dhis2_export(df, mode=cleaner_mode)
                if cleaning_report.column_rename_map:
                    schema = apply_rename_map_to_schema(tpr_analyzer._schema or {}, cleaning_report.column_rename_map)
                    tpr_analyzer._schema = schema
                    state_manager.update_state({"column_schema": schema})
                logger.info("[TPR-START] Applied cleaner to raw re-read (mode=%s)", cleaner_mode)
            except Exception as exc:
                logger.exception("[TPR-START] Cleaner failed: %s", exc)

    if df is None:
        raise TPRStartError("Could not load data file. Please re-upload your dataset.")

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


def run_analysis_message(session_id: str, message: str) -> dict:
    """Run the sync Data Analysis V3 orchestration and return a plain result dict."""
    from app.agent.agent import DataAnalysisAgent
    from app.agent.state_manager import DataAnalysisStateManager

    state_manager = DataAnalysisStateManager(session_id)
    current_state = state_manager.get_state() or {}
    lower_message = (message or "").lower().strip()
    is_tpr_active = state_manager.is_tpr_workflow_active()

    if current_state.get("workflow_transitioned"):
        logger.info("Workflow transitioned for session %s - staying in V3 agent mode", session_id)

    if is_tpr_active:
        logger.info("[CHAT] TPR active for session %s", session_id)
        tpr_language = TPRLanguageInterface(session_id)
        try:
            tpr_language.update_from_metadata(current_state)
        except Exception:
            pass
        return handle_tpr_active(session_id, message, state_manager, current_state, tpr_language)

    start_triggers = ["start tpr", "start the tpr", "tpr workflow", "run tpr"]
    if any(trigger in lower_message for trigger in start_triggers):
        try:
            return handle_tpr_start(session_id, message, state_manager, current_state)
        except TPRStartError as exc:
            return {"success": False, "message": str(exc), "session_id": session_id, "workflow": "tpr"}

    agent = DataAnalysisAgent(session_id)
    workflow_context = build_general_workflow_context(session_id)
    logger.info(
        "[AGENT CONTEXT] Session %s -> stage=%s columns=%d",
        session_id,
        workflow_context.get("stage"),
        len(workflow_context.get("data_columns") or []),
    )
    return _run_agent_sync(agent, message, workflow_context=workflow_context)


def stream_analysis_events(session_id: str, message: str) -> Iterator[dict[str, Any]]:
    """Yield Data Analysis V3 orchestration events for SSE callers."""
    from app.agent.agent import DataAnalysisAgent
    from app.agent.state_manager import DataAnalysisStateManager

    yield {"type": "status", "status": "started"}

    state_manager = DataAnalysisStateManager(session_id)
    current_state = state_manager.get_state() or {}
    lower_message = (message or "").lower().strip()
    is_tpr_active = state_manager.is_tpr_workflow_active()

    if current_state.get("workflow_transitioned"):
        logger.info("Workflow transitioned for session %s - staying in V3 agent mode", session_id)

    if is_tpr_active:
        tpr_language = TPRLanguageInterface(session_id)
        try:
            tpr_language.update_from_metadata(current_state)
        except Exception:
            pass
        try:
            result = handle_tpr_active(session_id, message, state_manager, current_state, tpr_language)
        except Exception as exc:
            logger.exception("[STREAM-TPR-ACTIVE] Error for session %s: %s", session_id, exc)
            result = {"success": False, "message": str(exc), "session_id": session_id, "workflow": "tpr"}
        yield {"type": "result", "data": result}
        return

    start_triggers = ["start tpr", "start the tpr", "tpr workflow", "run tpr"]
    if any(trigger in lower_message for trigger in start_triggers):
        try:
            result = handle_tpr_start(session_id, message, state_manager, current_state)
        except TPRStartError as exc:
            result = {"success": False, "message": str(exc), "session_id": session_id, "workflow": "tpr"}
        except Exception as exc:
            logger.exception("[STREAM-TPR-START] Unexpected error for session %s: %s", session_id, exc)
            result = {
                "success": False,
                "message": "Failed to start TPR workflow.",
                "session_id": session_id,
                "workflow": "tpr",
            }
        yield {"type": "result", "data": result}
        return

    workflow_context = build_general_workflow_context(session_id)
    agent = DataAnalysisAgent(session_id)
    yield from agent.analyze_stream(message, workflow_context=workflow_context)
