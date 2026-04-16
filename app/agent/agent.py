"""
LangGraph Agent for Data Analysis - CLEAN VERSION
Based on AgenticDataAnalysis backend.py and nodes.py
Pure agent pattern - NO TPR workflow logic
"""

import os
import logging
from typing import Literal, List, Dict, Any

import pandas as pd
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, BaseMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph
from langgraph.prebuilt import ToolNode

from .state import DataAnalysisState
from app.agent.tools.python_tool import analyze_data
from app.agent.prompts.system_prompt import MAIN_SYSTEM_PROMPT, TPR_WORKFLOW_GUIDANCE
from .encoding_handler import EncodingHandler
from .formatters import ResponseFormatter
from .data_loader import get_input_data
from .viz_processor import process_visualizations

logger = logging.getLogger(__name__)

# Optional memory service - graceful degradation if not available
try:
    from app.services.memory_service import get_memory_service
    HAS_MEMORY_SERVICE = True
except ImportError:
    HAS_MEMORY_SERVICE = False
    def get_memory_service():
        return None


class DataAnalysisAgent:
    """
    Main agent for data analysis using LangGraph.
    Follows AgenticDataAnalysis two-node pattern exactly.
    Uses OpenAI gpt-4o for high-quality analysis.

    NO TPR workflow logic - agent is pure.
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self._use_stub_model = False

        # Track conversation history early so stub mode can reuse it
        self.chat_history: List[BaseMessage] = []

        openai_key = os.environ.get('OPENAI_API_KEY')
        if not openai_key or not openai_key.startswith('sk-'):
            self._use_stub_model = True
            logger.warning("OPENAI_API_KEY missing or placeholder. Using stubbed LLM responses for Data Analysis V3.")
            self.llm = None
            self.tools = []
            self.chat_template = None
            self.model = None
            self.tool_node = None
            self.graph = None
        else:
            logger.info("Initializing Clean Data Analysis Agent with OpenAI gpt-4o")

            self.llm = ChatOpenAI(
                model="gpt-4o",
                api_key=openai_key,
                temperature=0.1,
                max_tokens=8000,
                timeout=90
            )

            # Set up tools - Python analysis + all specialized tools
            from app.agent.tools.map_tools import (
                create_variable_map,
                create_vulnerability_map,
                create_composite_score_maps,
                create_urban_extent_map,
                run_risk_analysis,
                plan_itn_distribution,
                switch_tpr_combination,
            )
            self.tools = [
                analyze_data,
                create_variable_map,
                create_vulnerability_map,
                create_composite_score_maps,
                create_urban_extent_map,
                run_risk_analysis,
                plan_itn_distribution,
                switch_tpr_combination,
            ]

            # Follow AgenticDataAnalysis pattern exactly
            model_with_tools = self.llm.bind_tools(
                self.tools,
                tool_choice="auto"
            )

            self.chat_template = ChatPromptTemplate.from_messages([
                ("system", MAIN_SYSTEM_PROMPT),
                ("placeholder", "{messages}"),
            ])

            self.model = self.chat_template | model_with_tools
            self.tool_node = ToolNode(self.tools)
            self.graph = self._build_graph()

        # Load persisted history for cross-worker continuity
        try:
            self._load_persisted_history()
        except Exception:
            pass

    def _load_persisted_history(self) -> None:
        """Load persisted chat messages into chat_history."""
        try:
            mem = get_memory_service()
            msgs = mem.get_messages(self.session_id)
            history: List[BaseMessage] = []
            for m in msgs:
                role = (m.get('role') or 'user').lower()
                content = m.get('content') or ''
                if not content:
                    continue
                if role == 'assistant':
                    history.append(AIMessage(content=content))
                else:
                    history.append(HumanMessage(content=content))
            if history:
                self.chat_history = history
        except Exception:
            # Non-fatal
            pass

    def _build_graph(self):
        """Build the LangGraph workflow - SIMPLIFIED to match original."""
        workflow = StateGraph(DataAnalysisState)

        # Add nodes - just 2 like original (agent, tools)
        workflow.add_node('agent', self._agent_node)
        workflow.add_node('tools', self._tools_node)

        # Routing - simplified like original
        workflow.add_conditional_edges('agent', self._route_from_agent)
        workflow.add_edge('tools', 'agent')  # Hardcoded edge back to agent

        workflow.set_entry_point('agent')  # Start at agent, not planner

        return workflow.compile()

    def _smart_truncate_messages(self, messages: List[BaseMessage], keep: int) -> List[BaseMessage]:
        """
        Smart message truncation that preserves tool_calls/tool message pairs.

        OpenAI requires:
        - If message has role='tool', it MUST be preceded by role='assistant' with tool_calls
        - We cannot truncate in a way that breaks this pairing

        Strategy:
        - Work backwards from end
        - Keep last N messages
        - If truncation would split a tool_calls/tool pair, include the pair
        """
        if len(messages) <= keep:
            return messages

        # Take last N messages
        result = messages[-keep:]

        # Check if first message in result is a ToolMessage
        # If so, we need to include the preceding AIMessage with tool_calls
        if result and isinstance(result[0], ToolMessage):
            # Find the corresponding AI message with tool_calls
            for i in range(len(messages) - keep - 1, -1, -1):
                msg = messages[i]
                if isinstance(msg, AIMessage) and hasattr(msg, 'tool_calls') and msg.tool_calls:
                    # Found it - include this message
                    result = [msg] + result
                    logger.info(f"[SMART TRUNCATION] Added AIMessage with tool_calls to preserve pair")
                    break

        return result

    @staticmethod
    def _build_value_profile(df, max_chars: int = 20000) -> str:
        """Build a rich value profile from a DataFrame for LLM context.

        Includes exact values for categorical columns, numeric ranges,
        null counts, and sample rows — so the LLM never has to guess
        about what the data actually contains.
        """
        import pandas as pd

        lines: list[str] = []
        lines.append(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        lines.append("")
        lines.append("## Column Profiles")

        for col in df.columns:
            dtype = df[col].dtype
            null_count = int(df[col].isna().sum())
            null_pct = null_count / max(len(df), 1) * 100
            null_note = f", {null_pct:.0f}% null" if null_pct > 5 else ""

            if dtype == 'object':
                unique_vals = df[col].dropna().unique()
                n_unique = len(unique_vals)
                if n_unique <= 25:
                    vals_str = ", ".join(sorted(str(v) for v in unique_vals))
                    lines.append(f"- **{col}** (text, {n_unique} unique{null_note}): [{vals_str}]")
                else:
                    top5 = df[col].value_counts(dropna=True).head(5)
                    top5_str = ", ".join(f"{idx}" for idx in top5.index)
                    lines.append(f"- **{col}** (text, {n_unique} unique{null_note}): top values: [{top5_str}]")
            elif pd.api.types.is_numeric_dtype(dtype):
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    lines.append(
                        f"- **{col}** (numeric{null_note}): "
                        f"min={col_data.min()}, max={col_data.max()}, mean={col_data.mean():.1f}"
                    )
                else:
                    lines.append(f"- **{col}** (numeric, all null)")
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    lines.append(
                        f"- **{col}** (date{null_note}): "
                        f"range {col_data.min()} to {col_data.max()}"
                    )
                else:
                    lines.append(f"- **{col}** (date, all null)")
            else:
                lines.append(f"- **{col}** ({dtype}{null_note})")

            # Check total length — stop adding columns if we're getting too big
            if sum(len(l) for l in lines) > max_chars - 1500:
                remaining = len(df.columns) - len([l for l in lines if l.startswith("- **")])
                if remaining > 0:
                    lines.append(f"- ... and {remaining} more columns")
                break

        # Add sample rows (3 rows, truncated values)
        lines.append("")
        lines.append("## Sample Rows (first 3)")
        try:
            sample = df.head(3)
            # Build a compact markdown table
            headers = list(sample.columns)
            header_line = "| " + " | ".join(str(h)[:30] for h in headers) + " |"
            sep_line = "| " + " | ".join("---" for _ in headers) + " |"
            lines.append(header_line)
            lines.append(sep_line)
            for _, row in sample.iterrows():
                vals = [str(v)[:30] if pd.notna(v) else "null" for v in row]
                lines.append("| " + " | ".join(vals) + " |")
        except Exception:
            lines.append("(Could not generate sample rows)")

        result = "\n".join(lines)
        # Final safety cap
        if len(result) > max_chars:
            result = result[:max_chars] + "\n... (profile truncated)"
        return result

    def _create_data_summary(self, state: DataAnalysisState) -> str:
        """Create summary of available data with full value profiles.

        The LLM receives exact column values, numeric ranges, and sample
        rows so it can write correct code on the first try — no guessing
        about capitalisation, value formats, or column contents.
        """
        summary = ""
        variables = []

        for data_obj in state.get("input_data", []):
            var_name = data_obj.get('variable_name', 'df')
            variables.append(var_name)

            summary += f"\n\nVariable: {var_name}\n"

            # If we have the actual DataFrame, build a rich profile
            df = data_obj.get('data')
            if df is not None and hasattr(df, 'columns') and len(df) > 0:
                summary += self._build_value_profile(df)
            else:
                # Fallback: just column names (old behaviour)
                summary += f"Description: {data_obj.get('data_description', 'Dataset loaded')}\n"
                if 'columns' in data_obj and data_obj['columns']:
                    cols = data_obj['columns']
                    if len(cols) <= 10:
                        summary += f"Columns: {', '.join(cols)}"
                    else:
                        summary += f"Columns ({len(cols)} total): {', '.join(cols[:10])}..."

        # Include any remaining variables from state
        if "current_variables" in state:
            remaining = [v for v in state["current_variables"] if v not in variables]
            for v in remaining:
                summary += f"\n\nVariable: {v}"

        if summary:
            summary += "\n\n**This data is loaded and ready for analysis. Use the exact column names and values shown above.**"

        # Append DHIS2 cleaning notes if the cleaner ran on this session's data.
        # This tells the LLM about column renames and duplicate merges so it can
        # explain to the user why numbers may differ from their original Excel.
        try:
            cleaning_note = self._build_cleaning_note()
            if cleaning_note:
                summary += "\n\n" + cleaning_note
        except Exception as exc:
            logger.debug(f"Could not build cleaning note: {exc}")

        # Append multi-year context if this is a multi-year dataset
        try:
            session_folder = f"instance/uploads/{self.session_id}"
            my_ctx = self._build_multi_year_context(session_folder)
            if my_ctx:
                summary += "\n\n" + my_ctx
        except Exception as exc:
            logger.debug(f"Could not build multi-year context: {exc}")

        return summary

    def _build_multi_year_context(self, session_folder: str) -> str:
        """Return multi-year awareness context for the LLM if this is a multi-year session."""
        import json
        ts_path = os.path.join(session_folder, 'tpr_time_series.csv')
        if not os.path.exists(ts_path):
            return ''
        try:
            import pandas as pd
            ts_df = pd.read_csv(ts_path, nrows=5000)
            if 'Period' not in ts_df.columns:
                return ''
            years = sorted(ts_df['Period'].dropna().unique())
            if len(years) <= 1:
                return ''

            status_path = os.path.join(session_folder, 'multi_year_status.json')
            bg_status = 'unknown'
            year_detail: dict = {}
            if os.path.exists(status_path):
                with open(status_path) as f:
                    s = json.load(f)
                bg_status = s.get('status', 'unknown')
                year_detail = s.get('detail', {})

            ready_years = [
                y for y in years
                if os.path.exists(os.path.join(session_folder, f'unified_dataset_{y}.csv'))
            ]
            trend_ready = os.path.exists(os.path.join(session_folder, 'trend_summary.csv'))

            ctx = f'\nMULTI-YEAR DATA: {years[0]}–{years[-1]} ({len(years)} years)\n'
            ctx += f'Available years: {list(years)}\n'
            ctx += f'Risk analysis ready for years: {ready_years or "computing in background"}\n'
            ctx += f'Trend summary ready (slope/direction/delta per ward): {trend_ready}\n'
            ctx += (
                'tpr_time_series.csv available for open-ended trend analysis '
                '(WardName, LGA, Period, Burden, Total_Positive, Total_Tested, TPR)\n'
            )
            ctx += (
                'Use analyze_data tool against tpr_time_series.csv for any trend question: '
                'year comparisons, threshold crossings, volatility, LGA aggregates, '
                'statistical significance, custom time ranges.\n'
            )
            ctx += f'Background computation status: {bg_status}\n'
            if year_detail:
                ctx += f'Per-year status: {year_detail}\n'
            return ctx
        except Exception:
            return ''

    def _build_cleaning_note(self) -> str:
        """Read cleaning_report.json from session folder and format notes for LLM."""
        import json
        report_path = f"instance/uploads/{self.session_id}/cleaning_report.json"
        if not os.path.exists(report_path):
            return ""
        try:
            with open(report_path) as f:
                report = json.load(f)
        except Exception:
            return ""

        if not report.get('cleaning_applied'):
            return ""

        lines = ["**Data cleaning notes (DHIS2 export):**"]

        renames = report.get('mojibake_fixed') or []
        if renames:
            lines.append(f"- Fixed {len(renames)} corrupted column name(s):")
            for r in renames[:5]:
                lines.append(f"    • `{r['from']}` → `{r['to']}`")

        merges = report.get('duplicates_merged') or []
        if merges:
            lines.append(f"- Merged {len(merges)} duplicate column group(s):")
            for m in merges[:5]:
                base = (m.get('base_column') or '')[:60]
                strategy = m.get('strategy', 'merge')
                lines.append(f"    • `{base}` ({strategy})")

        warnings = report.get('data_quality_warnings') or []
        if warnings:
            lines.append(f"- {len(warnings)} data quality warning(s) — see cleaning_report.json")

        if len(lines) == 1:
            return ""  # Only the header, no useful content
        return "\n".join(lines)

    def _agent_node(self, state: DataAnalysisState):
        """Agent node - calls GPT-4o with tools."""
        logger.info(f"[_AGENT_NODE] 🧠 Agent node called")
        logger.info(f"[_AGENT_NODE] State keys: {list(state.keys())}")
        logger.info(f"[_AGENT_NODE] Incoming messages count: {len(state.get('messages', []))}")

        # Create data context message
        logger.info(f"[_AGENT_NODE STEP 1] Creating data summary...")
        current_data_template = "The following data is available:\n{data_summary}"
        data_summary = self._create_data_summary(state)
        logger.info(f"[_AGENT_NODE STEP 1] Data summary length: {len(data_summary)} characters")

        current_data_message = HumanMessage(
            content=current_data_template.format(data_summary=data_summary)
        )
        logger.info(f"[_AGENT_NODE STEP 1] ✅ Data context message created")

        # Message truncation — keep last 20 messages
        # GPT-4o has 128K context, 20 messages is well within budget
        # CRITICAL: Must preserve tool_calls/tool message pairs for OpenAI API
        messages = list(state.get("messages", []))
        if len(messages) > 20:
            logger.warning(f"[_AGENT_NODE MESSAGE TRUNCATION] {len(messages)} messages — truncating to 20")

            first_msg = messages[0]
            is_workflow_context = isinstance(first_msg, HumanMessage) and '[WORKFLOW CONTEXT]' in first_msg.content

            if is_workflow_context:
                truncated = self._smart_truncate_messages(messages[1:], keep=19)
                messages = [messages[0]] + truncated
            else:
                messages = self._smart_truncate_messages(messages, keep=20)

            logger.info(f"[_AGENT_NODE MESSAGE TRUNCATION] Truncated: {len(state.get('messages', []))} → {len(messages)} messages")

        # Prepend data context to messages (for the model only - don't modify state!)
        logger.info(f"[_AGENT_NODE STEP 2] Prepending data context to messages...")
        messages_for_model = [current_data_message] + messages
        logger.info(f"[_AGENT_NODE STEP 2] ✅ Messages for model: {len(messages_for_model)}")

        # Call the model with modified messages (not the state!)
        logger.info(f"[_AGENT_NODE STEP 3] 🤖 Calling self.model.invoke()...")
        logger.info(f"[_AGENT_NODE STEP 3] ⚠️  THIS CALLS THE LLM - WATCH FOR ERRORS")
        try:
            import time
            start = time.time()
            # Create a copy of state with modified messages for the model
            state_for_model = dict(state)
            state_for_model["messages"] = messages_for_model
            llm_outputs = self.model.invoke(state_for_model)
            elapsed = time.time() - start
            logger.info(f"[_AGENT_NODE STEP 3] ✅ Model invoked successfully in {elapsed:.2f}s")
            logger.info(f"[_AGENT_NODE STEP 3] Response type: {type(llm_outputs).__name__}")
        except Exception as e:
            logger.error(f"[_AGENT_NODE STEP 3] ❌ Model invoke failed: {e}", exc_info=True)
            raise

        logger.info(f"[_AGENT_NODE STEP 4] Returning node result")
        return {
            "messages": [llm_outputs],
            "intermediate_outputs": [current_data_message.content]
        }

    def _tools_node(self, state: DataAnalysisState):
        """
        Tools node - executes tool calls.
        SIMPLIFIED to match original - just invoke tools.
        """
        # Add session_id to state for tools to access
        state_with_session = {**state, "session_id": self.session_id}

        # Execute tools
        result = self.tool_node.invoke(state_with_session)

        # Update guardrail counters — MUST return in result dict for LangGraph to see
        new_tool_count = state.get('tool_call_count', 0) + 1

        # Check if tool result contains an execution error (precise marker)
        new_error_count = state.get('consecutive_error_count', 0)
        messages = result.get('messages', [])
        if messages:
            content = getattr(messages[-1], 'content', str(messages[-1]))
            if '⚠️ **Execution Error:**' in content or 'Timeout:' in content:
                new_error_count += 1
            else:
                new_error_count = 0  # Reset on success

        result['tool_call_count'] = new_tool_count
        result['consecutive_error_count'] = new_error_count
        return result

    def _route_from_tools(
        self,
        state: DataAnalysisState,
    ) -> Literal['agent', '__end__']:
        """
        Decide next hop after tools execute.
        FIX: Check guardrails BEFORE routing back to agent to prevent infinite loops.
        """
        tool_call_count = state.get('tool_call_count', 0)
        consecutive_error_count = state.get('consecutive_error_count', 0)

        # Check tool call limit
        if tool_call_count >= 10:
            logger.warning(f"[ROUTE_FROM_TOOLS] Tool call limit reached ({tool_call_count}/10)")
            # Add fallback message explaining the issue
            fallback_msg = (
                "I've tried multiple approaches but haven't been able to complete this analysis. "
                "This might be due to:\n\n"
                "- Missing data columns or incorrect column names\n"
                "- Data type incompatibilities\n"
                "- Complex operations that need to be broken down\n\n"
                "Could you please:\n"
                "1. Verify the data structure matches what's expected\n"
                "2. Try a simpler version of the analysis\n"
                "3. Break the request into smaller steps"
            )
            state["messages"] = state.get("messages", []) + [AIMessage(content=fallback_msg)]
            return '__end__'

        # Check consecutive error limit (same error 3+ times = stuck loop)
        if consecutive_error_count >= 3:
            logger.warning(f"[ROUTE_FROM_TOOLS] Consecutive error limit reached ({consecutive_error_count}/3) - stuck loop detected")
            # Get the last error for context
            last_errors = [msg.content for msg in reversed(state.get("messages", []))
                          if isinstance(msg, ToolMessage) and "⚠️ **Execution Error:**" in msg.content]
            error_detail = last_errors[0] if last_errors else "Unknown error"

            fallback_msg = (
                "I'm encountering the same error repeatedly, which suggests this approach isn't working. "
                f"The error is:\n\n{error_detail}\n\n"
                "To resolve this, you could try:\n"
                "- Using different column names or checking available columns\n"
                "- Simplifying the analysis\n"
                "- Providing more specific details about what you want to analyze"
            )
            state["messages"] = state.get("messages", []) + [AIMessage(content=fallback_msg)]
            return '__end__'

        # Guardrails passed - continue to agent
        return 'agent'

    def _route_from_agent(
        self,
        state: DataAnalysisState,
    ) -> Literal['tools', '__end__']:
        """
        Decide next hop after agent responds.
        SIMPLIFIED to match original - just check for tool calls.
        """
        messages = state.get('messages', [])
        if not messages:
            raise ValueError('No messages found in state')

        ai_message = messages[-1]

        # If agent wants to use tools, route to tools node
        if hasattr(ai_message, 'tool_calls') and ai_message.tool_calls:
            return 'tools'

        # Otherwise, we're done
        return '__end__'

    def _get_input_data(self) -> List[Dict[str, Any]]:
        """Load most comprehensive dataset available for data-aware responses."""
        return get_input_data(self.session_id)

    def _process_visualizations(self, output_plots: List) -> List[Dict[str, Any]]:
        """Process output plots (pickle files) into visualization objects."""
        return process_visualizations(self.session_id, output_plots)

    async def analyze(self, user_query: str, workflow_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Main entry point for data analysis.

        CLEAN AND SIMPLE - like AgenticDataAnalysis user_sent_message()

        NO TPR workflow logic here.
        NO hardcoded responses.
        NO keyword extraction.

        Args:
            user_query: User's question/request
            workflow_context: Optional context about active workflow (stage, options, etc.)

        Just pass query to LangGraph and let GPT-4o handle everything.
        """
        logger.info(f"=" * 100)
        logger.info(f"[AGENT] 🤖 AGENT.ANALYZE() CALLED")
        logger.info(f"[AGENT] Session: {self.session_id}")
        logger.info(f"[AGENT] Query: '{user_query[:200]}...'")
        logger.info(f"[AGENT] Workflow context present: {workflow_context is not None}")
        logger.info(f"=" * 100)

        if self._use_stub_model:
            logger.info("[AGENT] Using stub LLM pathway")
            return await self._analyze_with_stub(user_query, workflow_context)

        if workflow_context:
            logger.info(f"[AGENT] Workflow context details: {workflow_context}")

        # Load input data
        logger.info(f"[AGENT STEP 1] Loading input data via _get_input_data()...")
        try:
            input_data_list = self._get_input_data()
            logger.info(f"[AGENT STEP 1] ✅ Input data loaded: {len(input_data_list)} datasets")
            if input_data_list:
                logger.info(f"[AGENT STEP 1] First dataset keys: {list(input_data_list[0].keys())}")
        except Exception as e:
            logger.error(f"[AGENT STEP 1] ❌ Failed to load input data: {e}", exc_info=True)
            raise

        # DEBUG: Add debug info to response
        debug_info = {
            "session_folder": f"instance/uploads/{self.session_id}",
            "session_folder_exists": os.path.exists(f"instance/uploads/{self.session_id}"),
            "datasets_loaded": len(input_data_list),
            "cwd": os.getcwd()
        }

        if input_data_list:
            debug_info["first_dataset"] = {
                "name": input_data_list[0].get('variable_name'),
                "rows": len(input_data_list[0].get('data', [])) if 'data' in input_data_list[0] else 0,
                "columns": len(input_data_list[0].get('columns', []))
            }

        if not input_data_list:
            return {
                "success": False,
                "message": "No data found. Please upload a dataset first.",
                "session_id": self.session_id
            }

        # Add workflow context to system prompt if provided
        context_message = None
        tpr_guidance_message = None
        memory_message = None
        if workflow_context:
            stage = workflow_context.get('stage', 'unknown')
            options = workflow_context.get('valid_options', [])
            data_columns = workflow_context.get('data_columns', [])
            data_shape = workflow_context.get('data_shape', {})
            workflow_type = workflow_context.get('workflow', 'general')

            context_parts = ["[WORKFLOW CONTEXT]"]

            if workflow_type == 'tpr':
                context_parts.append(f"User is in the TPR workflow at stage '{stage}'. Keep the guided flow moving while answering follow-up questions fully.")
                if options:
                    context_parts.append(f"Relevant choices right now: {', '.join(options)}.")
                if data_shape:
                    rows = data_shape.get('rows', 'unknown')
                    cols = data_shape.get('cols', 'unknown')
                    context_parts.append(f"Dataset shape: {rows} rows x {cols} columns.")
                tpr_guidance_message = HumanMessage(content=TPR_WORKFLOW_GUIDANCE)
            else:
                if stage in {'initial_data_loaded', 'data_exploring'}:
                    context_parts.append("User just uploaded data. Provide a concise overview: state rows/columns, list a few representative columns, skip helper fields (e.g., fuzzy/match/token columns), avoid dumping raw tables, and remind them about the TPR workflow option.")
                else:
                    context_parts.append(f"User is exploring data (stage '{stage}'). Answer their question directly using analyze_data when needed. Prefer summaries over raw tables and hide helper columns unless requested.")
                if options:
                    context_parts.append(f"If options are needed, reference: {', '.join(options)}.")
                if data_shape:
                    rows = data_shape.get('rows', 'unknown')
                    cols = data_shape.get('cols', 'unknown')
                    context_parts.append(f"Dataset shape: {rows} rows x {cols} columns.")
                schema_desc = workflow_context.get('column_schema_description')
                if schema_desc:
                    context_parts.append(schema_desc)

            context_message = HumanMessage(content='\n'.join(context_parts))

            # Log what we're sending
            logger.info(f"[AGENT] Workflow context message created:")
            logger.info(f"   - Stage: {stage}")
            logger.info(f"   - Data columns: {len(data_columns)}")
            logger.info(f"   - First 5 columns: {data_columns[:5] if data_columns else 'None'}")

        if HAS_MEMORY_SERVICE:
            try:
                mem = get_memory_service()
                summary = mem.get_fact(self.session_id, 'conversation_summary')
                dataset_schema = mem.get_fact(self.session_id, 'dataset_schema_summary')
                mem_messages = mem.get_messages(self.session_id)
                memory_sections: List[str] = []
                if dataset_schema:
                    memory_sections.append("## Dataset Schema\n" + dataset_schema)
                if summary:
                    memory_sections.append("## Conversation Memory\n" + summary)
                if mem_messages:
                    snippets = []
                    for msg in mem_messages[-4:]:
                        role = (msg.get('role') or 'user').title()
                        content = (msg.get('content') or '').strip()
                        if content:
                            snippets.append(f"{role}: {content}")
                    if snippets:
                        memory_sections.append("## Recent Turns\n" + "\n".join(snippets))
                if memory_sections:
                    memory_message = HumanMessage(content='\n\n'.join(memory_sections))
            except Exception as exc:
                logger.warning(f"[AGENT] Unable to prepare memory message: {exc}")

        # Persist the user message and refresh in-memory history
        try:
            mem = get_memory_service()
            _msgs = mem.get_messages(self.session_id)
            if not _msgs or _msgs[-1].get('role') != 'user' or (_msgs[-1].get('content') or '') != (user_query or ''):
                mem.append_message(self.session_id, 'user', user_query)
            self._load_persisted_history()
        except Exception:
            pass

        # Create input state (like AgenticDataAnalysis)
        messages = self.chat_history.copy()
        if context_message:
            messages.insert(0, context_message)
        if tpr_guidance_message:
            messages.insert(0, tpr_guidance_message)
        if memory_message:
            messages.insert(0, memory_message)
        messages.append(HumanMessage(content=user_query))

        input_state = {
            "messages": messages,
            "session_id": self.session_id,
            "input_data": input_data_list,
            "intermediate_outputs": [],
            "current_variables": {},
            "output_plots": [],
            "insights": [],
            "errors": [],
            "tool_call_count": 0,
            "consecutive_error_count": 0,
        }

        try:
            # Invoke graph - SIMPLIFIED like original
            logger.info(f"[AGENT] Invoking graph with {len(messages)} messages")

            import time
            start_time = time.time()

            # Use original's recursion limit
            result = self.graph.invoke(input_state, {"recursion_limit": 25})

            elapsed = time.time() - start_time
            logger.info(f"[AGENT] Graph completed in {elapsed:.2f} seconds")

            # Update chat history
            self.chat_history = result.get("messages", [])

            # Extract final response
            final_message = self.chat_history[-1] if self.chat_history else None

            # Process visualizations
            visualizations = self._process_visualizations(result.get("output_plots", []))

            logger.info(f"[AGENT] Analysis complete, {len(visualizations)} visualizations")

            # Persist assistant message
            try:
                mem.append_message(self.session_id, 'assistant', (final_message.content if final_message else 'Analysis complete.'))
            except Exception:
                pass

            final_content = final_message.content if final_message else "Analysis complete."
            final_content = ResponseFormatter.normalize_spacing(final_content)

            # Append visualization titles to the message so they appear in
            # conversation history — the LLM can then reference them on follow-ups.
            if visualizations:
                viz_descriptions = [v.get('title', 'Visualization') for v in visualizations]
                final_content += "\n\n**Visualizations shown:** " + "; ".join(viz_descriptions)

            return {
                "success": True,
                "message": final_content,
                "visualizations": visualizations,
                "session_id": self.session_id
            }

        except Exception as e:
            logger.error(f"[CLEAN AGENT] Error during analysis: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Error during analysis: {str(e)}",
                "session_id": self.session_id
            }

    async def _analyze_with_stub(self, user_query: str, workflow_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Offline-friendly pathway that returns deterministic responses for tests."""
        try:
            input_data_list = self._get_input_data()
        except Exception as exc:
            logger.error(f"[STUB] Failed to load data: {exc}", exc_info=True)
            input_data_list = []

        if not input_data_list:
            return {
                "success": False,
                "message": "No data found. Please upload a dataset first.",
                "session_id": self.session_id
            }

        first_dataset = next(
            (ds for ds in input_data_list if isinstance(ds.get('data'), pd.DataFrame)),
            None
        )
        df = first_dataset['data'] if first_dataset else None

        response_text = self._build_stub_response(user_query, df)

        self.chat_history.append(HumanMessage(content=user_query))
        self.chat_history.append(AIMessage(content=response_text))

        return {
            "success": True,
            "message": response_text,
            "visualizations": [],
            "session_id": self.session_id
        }

    def _build_stub_response(self, user_query: str, df: pd.DataFrame) -> str:
        """Generate a deterministic textual response without calling an external LLM."""
        query = (user_query or "").strip()
        query_lower = query.lower()

        if df is None or df.empty:
            return "I loaded your workspace but the dataset appears empty. Please confirm the upload and try again."

        row_count, col_count = df.shape
        columns = df.columns.tolist()
        preview_cols = ", ".join(columns[:5]) if columns else "(no columns found)"
        extra_cols = max(col_count - 5, 0)

        if "show me" in query_lower and "data" in query_lower:
            helper_markers = ("fuzzy", "match", "token", "hash", "tmp", "helper")
            visible_columns = [
                col for col in columns
                if not any(marker in col.lower() for marker in helper_markers)
            ]
            helper_count = len(columns) - len(visible_columns)
            if not visible_columns:
                visible_columns = columns

            sample_cols = visible_columns[:5]
            remaining_cols = max(len(visible_columns) - len(sample_cols), 0)

            lines = [
                "Here's a quick overview of your dataset:",
                f"- Rows: {row_count:,}",
                f"- Columns: {col_count}",
                "- Sample columns: " + ", ".join(sample_cols) + (f" (+ {remaining_cols} more)" if remaining_cols else "")
            ]
            if helper_count > 0:
                lines.append(f"- Note: {helper_count} helper columns (e.g., fuzzy/match fields) were omitted from this preview.")

            lines.extend([
                "",
                "Option 1 – Guided TPR Analysis (start the TPR workflow for test positivity and risk insights).",
                "Option 2 – Flexible Data Exploration (ask any question about the data or request charts).",
                "You can type 'start the tpr workflow' whenever you'd like me to run the malaria TPR analysis."
            ])

            return "\n".join(lines)

        if query_lower in {"1", "option 1", "start tpr", "start the tpr workflow", "tpr"}:
            return (
                "Great—starting the TPR workflow. We'll begin by picking a facility level. Options include Primary, "
                "Secondary, Tertiary, or All. Just tell me which one you prefer, and I'll keep the guided flow moving."
            )

        if query_lower in {"2", "option 2"}:
            return (
                "No problem—we'll stay in flexible data exploration mode. Ask me about any variables, statistics, or "
                "visualisations you need. If you later want the malaria TPR workflow, just say 'start the tpr workflow'."
            )

        if "top" in query_lower and "test" in query_lower:
            if 'total_tested' in df.columns:
                sorted_df = df.sort_values('total_tested', ascending=False).head(5)
                lines = ["Top 5 facilities by total tests:"]
                for idx, row in enumerate(sorted_df.itertuples(index=False), 1):
                    facility = getattr(row, 'healthfacility', getattr(row, 'wardname', 'Facility'))
                    total = getattr(row, 'total_tested', None)
                    lines.append(f"{idx}. {facility}: {int(total):,} tests")
                return "\n".join(lines)

            # Fallback to first numeric column if total_tested absent
            numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
            if numeric_cols:
                column = numeric_cols[0]
                sorted_df = df.sort_values(column, ascending=False).head(5)
                lines = [f"Top 5 records by {column}:"]
                for idx, row in enumerate(sorted_df.itertuples(index=False), 1):
                    label = getattr(row, 'wardname', getattr(row, 'healthfacility', f"Row {idx}"))
                    value = getattr(row, column)
                    lines.append(f"{idx}. {label}: {value}")
                return "\n".join(lines)

        # Default fallback message
        return (
            "I'm set up in offline test mode. Ask me to summarise the data, calculate rankings, or type 'start the tpr "
            "workflow' to jump into the guided malaria flow."
        )
