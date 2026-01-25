import json
"""
True py-sidebot Implementation for ChatMRPT

Clean implementation following py-sidebot's actual pattern:
1. Create chat session with system prompt
2. Register actual Python functions as tools
3. Pass user messages directly to chat session
4. Let LLM handle tool selection and execution

Key components preserved:
- Memory integration
- Visualization explanation
- Data schema handling
- Conversational data access
- Streaming support
- Special workflows (permissions, forks)
"""

import logging
import json
import time
import os
import math
import pandas as pd
from typing import Dict, Any, List, Optional
from flask import current_app

logger = logging.getLogger(__name__)

# Arena routing now handled by Mistral in analysis_routes.py
# This avoids duplicate routing logic and competing decision systems
ARENA_INTEGRATION_AVAILABLE = False  # Disabled to prevent duplicate routing


class RequestInterpreter:
    
    @staticmethod
    def force_session_save():
        """Force Flask session to save when using Redis backend."""
        from flask import session
        # Mark all keys as modified to force Redis save
        session.permanent = True
        session.modified = True
        # Touch all important keys
        for key in ['data_loaded', 'csv_loaded', 'shapefile_loaded']:
            if key in session:
                session[key] = session[key]  # Re-assign to trigger modification
    """
    True py-sidebot inspired request interpreter for ChatMRPT.
    
    Simple approach: register Python functions as tools and let LLM choose.
    """
    
    def __init__(self, llm_manager, data_service, analysis_service, visualization_service):
        self.llm_manager = llm_manager
        self.data_service = data_service
        self.analysis_service = analysis_service
        self.visualization_service = visualization_service
        
        # py-sidebot approach: Simple conversation storage
        self.conversation_history = {}
        self.session_data = {}  # Store session data for access
        self._memory_summary_tracker: Dict[str, Dict[str, Any]] = {}
        self.choice_interpreter = None
        self.tool_intent_resolver = None
        
        # Initialize memory system if available
        try:
            from app.services.memory_service import get_memory_service
            self.memory = get_memory_service()
        except Exception as e:
            logger.debug(f"Memory service not available: {e}")
            self.memory = None
        
        # Initialize extracted services (context, prompt, orchestration)
        try:
            from .session_context_service import SessionContextService
            from .data_repository import DataRepository
            from .prompt_builder import PromptBuilder
            from .tool_runner import ToolRunner
            from .llm_orchestrator import LLMOrchestrator

            self.data_repo = DataRepository()
            self.context_service = SessionContextService(self.data_repo)
            self.prompt_builder = PromptBuilder()
            # Fallback mapping to legacy wrappers to retain compatibility
            self.tool_runner = ToolRunner(fallbacks={
                # Two-layer data architecture
                'query_data': self._query_data,
                'analyze_data': self._analyze_data_with_python,
                # Specialized tools
                'explain_analysis_methodology': self._explain_analysis_methodology,
                'run_malaria_risk_analysis': self._run_malaria_risk_analysis,
                'create_settlement_map': self._create_settlement_map,
                'show_settlement_statistics': self._show_settlement_statistics,
                'query_tpr_data': self._query_tpr_data,
                'switch_tpr_combination': self._switch_tpr_combination,
            })
            self.orchestrator = LLMOrchestrator()
        except Exception as e:
            logger.warning(f"Refactor services init failed (non-fatal): {e}")
            self.context_service = None
            self.prompt_builder = None
            self.tool_runner = None
            self.orchestrator = None

        try:
            from .choice_interpreter import ChoiceInterpreter

            self.choice_interpreter = ChoiceInterpreter(self.llm_manager)
        except Exception as e:
            logger.warning(f"ChoiceInterpreter init failed (non-fatal): {e}")
            self.choice_interpreter = None

        try:
            from .tool_intent_resolver import ToolIntentResolver

            self.tool_intent_resolver = ToolIntentResolver(self.llm_manager)
        except Exception as e:
            logger.warning(f"ToolIntentResolver init failed (non-fatal): {e}")
            self.tool_intent_resolver = None

        # Initialize conversational data access placeholder
        self.conversational_data_access = None
        
        # py-sidebot pattern: Register tools as actual Python functions
        self.tools = {}
        
        # Agent memory: Cache DataExplorationAgent instances per session for conversation memory
        self.data_agents = {}  # session_id -> DataExplorationAgent instance
        
        self._register_tools()

    def _store_conversation(self, session_id: str, user_message: str, assistant_response: str = "") -> None:
        """Persist conversation turns to in-memory cache and optional MemoryService."""
        if not session_id:
            return

        # Update lightweight in-memory cache for quick lookups
        history = self.conversation_history.setdefault(session_id, [])
        if user_message:
            history.append({"role": "user", "content": user_message})
        if assistant_response:
            history.append({"role": "assistant", "content": assistant_response})
        if len(history) > 40:
            self.conversation_history[session_id] = history[-40:]

        if not self.memory:
            return

        try:
            last_messages = self.memory.get_messages(session_id)
            last_role = last_messages[-1]['role'] if last_messages else None
            last_content = last_messages[-1]['content'] if last_messages else None
            if user_message:
                if not (last_role == 'user' and (last_content or '') == user_message):
                    self.memory.append_message(session_id, "user", user_message)
            if assistant_response:
                last_messages = self.memory.get_messages(session_id)
                last_role = last_messages[-1]['role'] if last_messages else None
                last_content = last_messages[-1]['content'] if last_messages else None
                if not (last_role == 'assistant' and (last_content or '') == assistant_response):
                    self.memory.append_message(session_id, "assistant", assistant_response)
            self._ensure_memory_summary(session_id)
        except Exception as exc:
            logger.warning(f"Memory append failed for session {session_id}: {exc}")

    def _ensure_memory_summary(self, session_id: str) -> None:
        """Use LLM to maintain a compact memory summary after new messages."""
        if not self.memory or not self.llm_manager:
            return

        tracker = self._memory_summary_tracker.get(session_id, {})
        last_count = tracker.get('count', 0)
        last_ts = tracker.get('ts', 0.0)

        try:
            messages = self.memory.get_messages(session_id)
        except Exception as exc:
            logger.warning(f"Could not read memory messages for {session_id}: {exc}")
            return

        if not messages:
            return

        # Avoid excessive summarisation: require at least 2 new messages or 60s elapsed
        if len(messages) - last_count < 2 and (time.time() - last_ts) < 60:
            return

        transcript = []
        for msg in messages[-8:]:
            role = (msg.get('role') or 'user').upper()
            content = (msg.get('content') or '').strip()
            if not content:
                continue
            transcript.append(f"{role}: {content}")

        if not transcript:
            return

        summary_prompt = "\n".join(transcript)
        system_message = (
            "You maintain a concise memory for an AI malaria analysis assistant. "
            "Summarise the key facts, data selections, results, outstanding questions, "
            "and user goals from the conversation transcript below. "
            "Limit to 5 short bullet points (<= 120 words total)."
        )

        try:
            summary_text = self.llm_manager.generate_response(
                prompt=f"Conversation transcript:\n{summary_prompt}\n\nProvide bullet points:",
                system_message=system_message,
                temperature=0.2,
                max_tokens=200,
                session_id=session_id
            )
        except Exception as exc:
            logger.warning(f"Memory summary generation failed for {session_id}: {exc}")
            return

        if not summary_text or summary_text.lower().startswith("error"):
            return

        summary_trimmed = summary_text.strip()
        if len(summary_trimmed) > 800:
            summary_trimmed = summary_trimmed[:800]

        try:
            self.memory.set_fact(session_id, 'conversation_summary', summary_trimmed)
            tracker.update({'count': len(messages), 'ts': time.time()})
            self._memory_summary_tracker[session_id] = tracker
        except Exception as exc:
            logger.warning(f"Failed to persist memory summary for {session_id}: {exc}")

    def _enrich_session_context_with_memory(self, session_id: str, session_context: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(session_context, dict):
            return session_context

        if not self.memory:
            return session_context

        enriched = dict(session_context)

        try:
            summary = self.memory.get_fact(session_id, 'conversation_summary')
            if summary:
                enriched['memory_summary'] = summary

            messages = self.memory.get_messages(session_id)
            if messages:
                recent = []
                for msg in messages[-4:]:
                    role = (msg.get('role') or 'user').title()
                    content = (msg.get('content') or '').strip()
                    if content:
                        recent.append(f"{role}: {content}")
                if recent:
                    enriched['recent_conversation'] = "\n".join(recent)
        except Exception as exc:
            logger.warning(f"Failed to enrich session context with memory for {session_id}: {exc}")

        return enriched

    def _list_dataset_columns(self, session_id: str, page: int = 1, page_size: int = 15) -> Dict[str, Any]:
        """Provide a clean paginated summary of dataset columns for the LLM."""
        if page_size <= 0:
            page_size = 15
        if page <= 0:
            page = 1

        column_details: List[Dict[str, Any]] = []
        if self.memory:
            try:
                stored = self.memory.get_fact(session_id, 'dataset_schema_columns')
                if isinstance(stored, list):
                    column_details = stored
            except Exception:
                column_details = []

        if not column_details:
            try:
                context_snapshot = self._get_session_context(session_id)
                column_details = context_snapshot.get('schema_columns', []) if context_snapshot else []
            except Exception:
                column_details = []

        if not column_details:
            return {
                'response': 'I could not find any dataset columns yet. Please upload data or run the analysis first.',
                'status': 'error',
                'tools_used': ['list_dataset_columns']
            }

        total = len(column_details)
        total_pages = max(1, math.ceil(total / page_size))
        page = min(page, total_pages)
        start = (page - 1) * page_size
        end = start + page_size
        chunk = column_details[start:end]

        # Clean header - only show pagination if multiple pages
        if total_pages > 1:
            lines = [f"**Dataset columns** ({total} columns, showing {start+1}-{min(end, total)}):"]
        else:
            lines = [f"**Dataset columns** ({total} total):"]

        for col in chunk:
            name = col.get('name', 'unknown')
            dtype = col.get('dtype', 'object')
            non_null = col.get('non_null', 'n/a')
            unique = col.get('unique', 'n/a')
            sample_values = col.get('sample_values') or []
            sample = ', '.join(sample_values) if sample_values else '–'
            lines.append(f"- **{name}** [{dtype}] – {non_null} non-null, {unique} unique")

        if page < total_pages:
            lines.append(f"\n*Showing columns {start+1}-{end} of {total}. Ask for more columns to see the rest.*")

        return {
            'response': "\n".join(lines),
            'status': 'success',
            'tools_used': ['list_dataset_columns']
        }

    def _query_data(self, session_id: str, query: str) -> Dict[str, Any]:
        """
        Simple text-to-SQL data query tool. Returns data only, NEVER generates charts.

        Use this tool for ALL simple data queries including:
        - "What are the top 10 highest risk wards?"
        - "Show me wards with composite_score > 0.5"
        - "What's the average TPR?"
        - "List all columns in my data"
        - "How many wards are in each LGA?"
        - "What's the correlation between X and Y?"

        This tool translates natural language to SQL and returns formatted text results.
        For visualizations (charts, plots, heatmaps), use analyze_data instead.

        Args:
            session_id: Session identifier
            query: Natural language data query

        Returns:
            Dict with formatted text response, no visualizations
        """
        logger.info(f"📊 TOOL: query_data called")
        logger.info(f"  Session: {session_id}")
        logger.info(f"  Query: {query[:100]}...")

        try:
            from app.services.conversational_data_access import ConversationalDataAccess
            cda = ConversationalDataAccess(session_id, self.llm_manager)

            # Check if query is asking for columns/variables
            query_lower = query.lower()
            is_column_query = any(term in query_lower for term in [
                'columns', 'variables', 'fields', 'what data', 'what variables',
                'list columns', 'show columns', 'available columns', 'dataset schema'
            ])

            if is_column_query:
                # Get comprehensive schema instead of paginated list
                schema = cda.generate_comprehensive_schema()
                if 'error' in schema:
                    return {
                        'response': f"Could not retrieve columns: {schema['error']}",
                        'status': 'error',
                        'tools_used': ['query_data']
                    }

                # Format all columns nicely
                columns = schema.get('columns', {})
                total = len(columns)
                lines = [f"**Your dataset has {total} columns:**\n"]

                for col_name, col_info in columns.items():
                    dtype = col_info.get('dtype', 'unknown')
                    non_null = col_info.get('non_null_count', 'n/a')
                    unique = col_info.get('unique_count', 'n/a')
                    lines.append(f"- **{col_name}** [{dtype}] – {non_null} non-null, {unique} unique")

                return {
                    'response': "\n".join(lines),
                    'status': 'success',
                    'tools_used': ['query_data']
                }

            # Use LLM to convert natural language to SQL
            schema = cda.generate_comprehensive_schema()
            if 'error' in schema:
                return {
                    'response': f"No data available: {schema['error']}",
                    'status': 'error',
                    'tools_used': ['query_data']
                }

            # Build SQL generation prompt with smarter column selection
            columns_list = list(schema.get('columns', {}).keys())

            # Identify key column types for better prompting
            ward_cols = [c for c in columns_list if 'ward' in c.lower()]
            score_cols = [c for c in columns_list if 'score' in c.lower() or 'composite' in c.lower()]
            rank_cols = [c for c in columns_list if 'rank' in c.lower()]
            category_cols = [c for c in columns_list if 'category' in c.lower() or 'vulnerability' in c.lower()]
            lga_cols = [c for c in columns_list if 'lga' in c.lower()]

            sql_prompt = f"""Convert this natural language query to SQL. The table name is 'df'.

Available columns: {', '.join(columns_list[:50])}

Key columns identified:
- Ward names: {ward_cols[:2] if ward_cols else 'Not found'}
- Scores: {score_cols[:2] if score_cols else 'Not found'}
- Rankings: {rank_cols[:2] if rank_cols else 'Not found'}
- Categories: {category_cols[:2] if category_cols else 'Not found'}
- LGA: {lga_cols[:2] if lga_cols else 'Not found'}

User query: {query}

IMPORTANT RULES:
1. For ranking/top-N queries: Select identifier + score + rank + category only (NOT SELECT *)
2. For count queries: Use COUNT(*) with meaningful alias
3. For aggregations: Use GROUP BY with clear aliases
4. For "why is X ranked" queries: SELECT * to get all details
5. Always include the primary identifier column (ward name, LGA, etc.)
6. NEVER use SELECT * for listing queries - select only relevant columns

Return ONLY the SQL query, nothing else. Use standard SQL syntax.

Examples:
- "top 10 highest risk wards" -> SELECT WardName, composite_score, composite_rank, vulnerability_category FROM df ORDER BY composite_score DESC LIMIT 10
- "how many high risk wards" -> SELECT COUNT(*) as high_risk_count FROM df WHERE vulnerability_category = 'High Risk'
- "average TPR by LGA" -> SELECT LGAName, AVG(tpr_mean) as avg_tpr FROM df GROUP BY LGAName
- "why is Abuja ranked high" -> SELECT * FROM df WHERE WardName LIKE '%Abuja%' OR ward_name LIKE '%Abuja%'
- "wards with score > 0.5" -> SELECT WardName, composite_score, vulnerability_category FROM df WHERE composite_score > 0.5

SQL:"""

            # Generate SQL using LLM
            sql_response = self.llm_manager.generate_response(
                prompt=sql_prompt,
                temperature=0.1,
                max_tokens=300
            )

            # Clean the SQL response
            sql_query = sql_response.strip()
            if sql_query.startswith('```'):
                lines_sql = sql_query.split('\n')
                sql_query = '\n'.join(lines_sql[1:-1])
            sql_query = sql_query.strip()

            logger.info(f"Generated SQL: {sql_query}")

            # Execute the SQL query with original query for better intent detection
            result = cda.process_sql_query(sql_query, original_query=query)

            if result.get('success'):
                return {
                    'response': result.get('output', 'Query executed successfully'),
                    'status': 'success',
                    'tools_used': ['query_data']
                }
            else:
                return {
                    'response': f"Query error: {result.get('error', 'Unknown error')}",
                    'status': 'error',
                    'tools_used': ['query_data']
                }

        except Exception as e:
            logger.error(f"Error in query_data: {e}", exc_info=True)
            return {
                'response': f"Error executing query: {str(e)}",
                'status': 'error',
                'tools_used': ['query_data']
            }

    def _query_tpr_data(self, session_id: str, facility_level: str = 'all',
                        age_group: str = 'all_ages', top_n: int = None,
                        lga: str = None, sort_by: str = 'tpr') -> Dict[str, Any]:
        """
        Query pre-computed TPR data for a specific facility level and age group combination.

        This tool allows users to explore different TPR combinations after completing
        the initial TPR workflow without re-uploading data.
        """
        try:
            from app.tools.tpr_query_tool import QueryTPRData

            # Create tool instance with parameters
            tool = QueryTPRData(
                facility_level=facility_level,
                age_group=age_group,
                top_n=top_n,
                lga=lga,
                sort_by=sort_by
            )

            # Execute the tool
            result = tool.execute(session_id)

            if result.success:
                return {
                    'response': result.message,
                    'status': 'success',
                    'data': result.data,
                    'tools_used': ['query_tpr_data']
                }
            else:
                return {
                    'response': result.message,
                    'status': 'error',
                    'error_details': result.error_details,
                    'tools_used': ['query_tpr_data']
                }

        except Exception as e:
            logger.error(f"Error in _query_tpr_data: {e}")
            return {
                'response': f"Failed to query TPR data: {str(e)}",
                'status': 'error',
                'tools_used': ['query_tpr_data']
            }

    def _switch_tpr_combination(self, session_id: str, facility_level: str = 'all',
                                age_group: str = 'all_ages', generate_map: bool = True) -> Dict[str, Any]:
        """
        Switch to a different TPR combination and regenerate analysis files.

        This tool rebuilds raw_data.csv, raw_shapefile.zip, and the TPR map for a new
        combination using cached ward data. Use after completing the TPR workflow when
        the user wants to analyze a different facility level or age group.

        Examples of user requests that should trigger this tool:
        - "Switch to TPR for pregnant women at secondary facilities"
        - "Show TPR map for under 5s at primary facilities"
        - "Use tertiary facility TPR"
        - "Change to all ages at primary facilities"
        - "Generate TPR map for over 5 age group"
        """
        try:
            from app.tools.tpr_query_tool import SwitchTPRCombination

            # Create tool instance with parameters
            tool = SwitchTPRCombination(
                facility_level=facility_level,
                age_group=age_group,
                generate_map=generate_map
            )

            # Execute the tool
            result = tool.execute(session_id)

            response = {
                'response': result.message,
                'status': 'success' if result.success else 'error',
                'data': result.data,
                'tools_used': ['switch_tpr_combination']
            }

            # Include visualization if present
            if result.success and hasattr(result, 'visualizations') and result.visualizations:
                response['visualizations'] = result.visualizations

            return response

        except Exception as e:
            logger.error(f"Error in _switch_tpr_combination: {e}")
            return {
                'response': f"Failed to switch TPR combination: {str(e)}",
                'status': 'error',
                'tools_used': ['switch_tpr_combination']
            }

    def _register_tools(self):
        """Register actual Python functions as tools - true py-sidebot style."""
        logger.info("Registering tools - py-sidebot pattern")

        # Register analysis tools
        self.tools['run_malaria_risk_analysis'] = self._run_malaria_risk_analysis
        # Disabled single-method tools to prevent confusion
        # self.tools['run_composite_analysis'] = self._run_composite_analysis
        # self.tools['run_pca_analysis'] = self._run_pca_analysis

        # Register visualization tools
        self.tools['create_vulnerability_map'] = self._create_vulnerability_map
        # REMOVED: create_box_plot - Tool #19 (analyze_data_with_python) can create matplotlib box plots
        # self.tools['create_box_plot'] = self._create_box_plot
        self.tools['create_pca_map'] = self._create_pca_map
        self.tools['create_variable_distribution'] = self._create_variable_distribution
        self.tools['create_urban_extent_map'] = self._create_urban_extent_map
        self.tools['create_decision_tree'] = self._create_decision_tree
        self.tools['create_composite_score_maps'] = self._create_composite_score_maps

        # Register settlement visualization tools
        self.tools['create_settlement_map'] = self._create_settlement_map
        self.tools['show_settlement_statistics'] = self._show_settlement_statistics

        # Register explanation tools
        self.tools['explain_analysis_methodology'] = self._explain_analysis_methodology

        # NEW: ITN Planning Tool
        self.tools['run_itn_planning'] = self._run_itn_planning

        # TWO-LAYER DATA ARCHITECTURE:
        # Layer 1: query_data - Simple text-to-SQL, returns data only, NO charts
        self.tools['query_data'] = self._query_data

        # Layer 2: analyze_data - Complex Python analysis, generates charts ONLY when explicitly requested
        self.tools['analyze_data'] = self._analyze_data_with_python

        # REMOVED: Redundant data tools (folded into query_data and analyze_data)
        # - execute_sql_query -> replaced by query_data
        # - list_dataset_columns -> replaced by query_data
        # - execute_data_query -> replaced by analyze_data
        # - run_data_quality_check -> replaced by analyze_data

        # TPR query tool - allows querying pre-computed TPR combinations
        self.tools['query_tpr_data'] = self._query_tpr_data

        # TPR combination switch - regenerate analysis files for different facility/age group
        self.tools['switch_tpr_combination'] = self._switch_tpr_combination

        logger.info(f"Registered {len(self.tools)} tools (two-layer data architecture: query_data for SQL, analyze_data for Python)")

    # ------------------------------------------------------------------
    # Tool intent routing helpers
    # ------------------------------------------------------------------
    def _get_session_state(self, session_id: str) -> Dict[str, Any]:
        state = self.session_data.setdefault(session_id, {})
        state.setdefault('history', [])
        state.setdefault('last_tool', None)
        state.setdefault('last_variable_distribution', None)
        state.setdefault('recent_variables', [])
        return state

    def _record_tool_invocation(self, session_id: str, tool_name: str, args: Dict[str, Any]) -> None:
        state = self._get_session_state(session_id)
        entry = {
            'tool': tool_name,
            'args': dict(args or {}),
            'timestamp': time.time(),
        }
        state['history'].append(entry)
        state['last_tool'] = tool_name
        if len(state['history']) > 40:
            state['history'] = state['history'][-40:]

        if tool_name == 'create_variable_distribution':
            variable = args.get('variable_name') or args.get('map_variable') or args.get('variable')
            if variable:
                state['last_variable_distribution'] = variable
                recent = state.setdefault('recent_variables', [])
                recent.append(variable)
                if len(recent) > 10:
                    state['recent_variables'] = recent[-10:]

    def _attempt_direct_tool_resolution(
        self,
        user_message: str,
        session_id: str,
        session_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not self.tool_intent_resolver:
            return None

        session_state = self._get_session_state(session_id)
        resolution = self.tool_intent_resolver.resolve(user_message, session_context, session_state)
        if not resolution:
            return None

        logger.info(
            "🧠 ToolIntentResolver match: %s (confidence=%.2f, reason=%s)",
            resolution.tool,
            resolution.confidence,
            resolution.reason,
        )

        final_args = self._finalize_arguments_for_tool(
            resolution,
            user_message,
            session_id,
            session_context,
            session_state,
        )
        if final_args is None:
            logger.info("🧠 Intent resolved to %s but arguments missing", resolution.tool)
            return None

        execution_result = self._execute_direct_tool(resolution, session_id, final_args)
        self._record_tool_invocation(session_id, resolution.tool, final_args)

        # Annotate debug metadata
        debug_block = execution_result.setdefault('debug', {})
        debug_block['intent_resolver'] = {
            'tool': resolution.tool,
            'confidence': resolution.confidence,
            'score': resolution.score,
            'reason': resolution.reason,
            'matched_terms': list(resolution.matched_terms),
            'final_args': final_args,
        }
        execution_result.setdefault('tools_used', [resolution.tool])

        return execution_result

    def _finalize_arguments_for_tool(
        self,
        resolution,
        user_message: str,
        session_id: str,
        session_context: Dict[str, Any],
        session_state: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        args = dict(resolution.inferred_args or {})

        # Use ChoiceInterpreter to refine/fill arguments when available
        need_choice = resolution.requires_args or (not args and resolution.supports_choice_interpreter)
        if self.choice_interpreter and resolution.supports_choice_interpreter and need_choice:
            try:
                columns_context = {
                    'columns': session_context.get('columns', []),
                    'schema_columns': session_context.get('schema_columns', []),
                }
                memory_summary = session_context.get('memory_summary')
                choice = self.choice_interpreter.resolve(
                    resolution.tool,
                    user_message,
                    memory_summary=memory_summary,
                    columns_context=columns_context,
                    session_id=session_id,
                )
                if choice and isinstance(choice.get('args'), dict):
                    choice_conf = float(choice.get('confidence', 0.0))
                    if choice_conf >= 0.35:
                        args.update(choice['args'])
            except Exception as exc:
                logger.warning(f"ChoiceInterpreter failed for {resolution.tool}: {exc}")

        normalized_args = self._normalize_tool_arguments(
            resolution.tool,
            args,
            user_message,
            session_state,
        )

        if resolution.requires_args and not normalized_args:
            return None

        return normalized_args

    def _normalize_tool_arguments(
        self,
        tool_name: str,
        args: Dict[str, Any],
        user_message: str,
        session_state: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Normalize raw argument dict into the signature expected by helpers."""
        normalized = {k: v for k, v in (args or {}).items() if v not in (None, "", [])}

        if tool_name == 'create_variable_distribution':
            candidate = (
                normalized.pop('map_variable', None)
                or normalized.get('variable')
                or normalized.get('variable_name')
            )
            if candidate:
                normalized['variable_name'] = candidate
            elif session_state.get('last_variable_distribution') and any(
                token in user_message.lower() for token in ['it', 'them', 'that', 'those', 'these']
            ):
                normalized['variable_name'] = session_state['last_variable_distribution']

        if tool_name == 'run_malaria_risk_analysis':
            variables = normalized.get('variables')
            if isinstance(variables, str):
                normalized['variables'] = [v.strip() for v in variables.split(',') if v.strip()]

        if tool_name == 'analyze_data_with_python':
            normalized.setdefault('query', user_message)

        if tool_name == 'list_dataset_columns':
            page = normalized.get('page')
            if isinstance(page, str) and page.isdigit():
                normalized['page'] = int(page)

        if tool_name == 'run_itn_planning':
            # Normalise shorthand like "200k"
            for key in ('total_nets', 'avg_household_size'):
                value = normalized.get(key)
                if isinstance(value, str):
                    cleaned = value.replace(',', '').strip()
                    multiplier = 1
                    if cleaned.lower().endswith('k'):
                        multiplier = 1000
                        cleaned = cleaned[:-1]
                    elif cleaned.lower().endswith('m'):
                        multiplier = 1_000_000
                        cleaned = cleaned[:-1]
                    try:
                        number = float(cleaned)
                        normalized[key] = int(number * multiplier)
                    except ValueError:
                        pass

        return normalized

    def _execute_direct_tool(
        self,
        resolution,
        session_id: str,
        args: Dict[str, Any],
    ) -> Dict[str, Any]:
        tool_name = resolution.tool

        try:
            if tool_name in self.tools:
                result = self.tools[tool_name](session_id, **args)
            else:
                payload = json.dumps({**args, 'session_id': session_id})
                result = self.tool_runner.execute(tool_name, payload)
        except Exception as exc:
            logger.error(f"Error executing resolved tool {tool_name}: {exc}")
            return {
                'status': 'error',
                'response': f"I ran into an issue while executing {tool_name}: {exc}",
                'tools_used': [tool_name],
            }

        normalized = self._normalize_tool_result(tool_name, result)
        return normalized

    def _normalize_tool_result(self, tool_name: str, raw_result: Any) -> Dict[str, Any]:
        if isinstance(raw_result, dict):
            response_text = raw_result.get('response') or raw_result.get('message') or ''
            normalized = dict(raw_result)
            normalized['response'] = response_text
            normalized.setdefault('message', response_text)
            normalized.setdefault('status', 'success')
            normalized.setdefault('tools_used', [tool_name])
            return normalized

        response_text = str(raw_result or '')
        return {
            'status': 'success',
            'response': response_text,
            'message': response_text,
            'tools_used': [tool_name],
        }
    
    def process_message(self, user_message: str, session_id: str, session_data: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
        """py-sidebot pattern: Pass message directly to LLM with tools."""
        start_time = time.time()
        
        try:
            # 🔍 DEBUG: Enhanced logging for workflow tracking
            logger.info("=" * 60)
            logger.info("📊 ANALYSIS: RequestInterpreter.process_message")
            logger.info(f"  📝 User Message: {user_message[:100]}...")
            logger.info(f"  🆔 Session ID: {session_id}")
            logger.info(f"  📂 Session Keys: {list(session_data.keys()) if session_data else 'None'}")
            logger.info(f"  🎯 Analysis Mode: {kwargs.get('is_data_analysis', False)}")
            logger.info(f"  🔄 Tab Context: {kwargs.get('tab_context', 'unknown')}")
            
            # Check session state
            from flask import session as flask_session
            logger.info("  📊 Session State:")
            logger.info(f"    - Analysis Complete: {flask_session.get('analysis_complete', False)}")
            logger.info(f"    - Data Loaded: {flask_session.get('data_loaded', False)}")
            logger.info(f"    - ITN Planning Complete: {flask_session.get('itn_planning_complete', False)}")
            logger.info(f"    - TPR Workflow Complete: {flask_session.get('tpr_workflow_complete', False)}")
            logger.info("=" * 60)
            
            # Handle special workflows first (pass kwargs for context flags)
            logger.info(f"🔄 Checking special workflows for: {user_message[:50]}...")
            special_result = self._handle_special_workflows(user_message, session_id, session_data, **kwargs)
            if special_result:
                return special_result
            
            # Get session context (service when available)
            session_context = self._get_session_context(session_id, session_data)
            session_context = self._enrich_session_context_with_memory(session_id, session_context)

            # Try deterministic intent resolution before handing off to LLM
            direct_result = self._attempt_direct_tool_resolution(
                user_message,
                session_id,
                session_context,
            )
            if direct_result:
                self._store_conversation(session_id, user_message, direct_result.get('response', ''))
                direct_result['total_time'] = time.time() - start_time
                return direct_result

            # Simple routing: no data = conversational, with data = tools available
            if not session_context.get('data_loaded', False):
                return self._simple_conversational_response(user_message, session_context, session_id)

            # Refactored orchestration path
            system_prompt = self._build_system_prompt_refactored(session_context, session_id)
            if self.tool_runner and self.orchestrator:
                function_schemas = self.tool_runner.get_function_schemas()
                result = self.orchestrator.run_with_tools(
                    self.llm_manager,
                    system_prompt,
                    user_message,
                    function_schemas,
                    session_id,
                    self.tool_runner,
                )
            else:
                # Fallback to legacy path
                result = self._llm_with_tools(user_message, session_context, session_id)
            
            # Store conversation
            self._store_conversation(session_id, user_message, result.get('response', ''))
            
            result['total_time'] = time.time() - start_time
            return result
            
        except Exception as e:
            logger.error(f"Error in py-sidebot processing: {e}")
            return {
                'status': 'error',
                'response': f'I encountered an issue: {str(e)}',
                'tools_used': []
            }
    
    def process_message_streaming(self, user_message: str, session_id: str, session_data: Dict[str, Any] = None, **kwargs):
        """Streaming version for better UX."""
        try:
            # Handle special workflows (pass kwargs for context flags)
            special_result = self._handle_special_workflows(user_message, session_id, session_data, **kwargs)
            if special_result:
                # CRITICAL: Include visualizations if present
                yield {
                    'content': special_result.get('response', ''),
                    'status': special_result.get('status', 'success'),
                    'visualizations': special_result.get('visualizations', []),
                    'download_links': special_result.get('download_links', []),
                    'tools_used': special_result.get('tools_used', []),
                    'done': True
                }
                return
            
            # Get session context
            session_context = self._get_session_context(session_id, session_data)
            session_context = self._enrich_session_context_with_memory(session_id, session_context)
            
            logger.info(f"🔍 Session context for {session_id}:")
            logger.info(f"   data_loaded: {session_context.get('data_loaded', False)}")
            logger.info(f"   has_csv: {session_context.get('csv_loaded', False)}")
            logger.info(f"   has_shapefile: {session_context.get('shapefile_loaded', False)}")
            logger.info(f"   current_data: {str(session_context.get('current_data', 'None'))[:100]}")
            logger.info(f"   session_data param keys: {list(session_data.keys())[:10] if session_data else 'None'}")
            logger.info(f"   kwargs: {kwargs}")

            direct_result = self._attempt_direct_tool_resolution(
                user_message,
                session_id,
                session_context,
            )
            if direct_result:
                self._store_conversation(session_id, user_message, direct_result.get('response', ''))
                yield {
                    'content': direct_result.get('response', ''),
                    'status': direct_result.get('status', 'success'),
                    'visualizations': direct_result.get('visualizations', []),
                    'download_links': direct_result.get('download_links', []),
                    'tools_used': direct_result.get('tools_used', []),
                    'debug': direct_result.get('debug'),
                    'done': True
                }
                return
            
            if not session_context.get('data_loaded', False):
                logger.info(f"❌ No data loaded, using conversational streaming")
                # Use streaming for conversational responses too
                if self.llm_manager and hasattr(self.llm_manager, 'generate_with_functions_streaming'):
                    system_prompt = self._build_system_prompt_refactored(session_context, session_id)
                    
                    # Stream the response
                    for chunk in self.llm_manager.generate_with_functions_streaming(
                        messages=[{"role": "user", "content": user_message}],
                        system_prompt=system_prompt,
                        functions=[],  # No tools for simple conversation
                        temperature=0.7,
                        session_id=session_id
                    ):
                        # Handle OpenAI streaming format (no 'type' field)
                        content = chunk.get('content', '')
                        if content:  # Only yield if there's actual content
                            yield {
                                'content': content,
                                'status': 'success',
                                'done': False
                            }
                    
                    # Send final done signal
                    yield {
                        'content': '',
                        'status': 'success',
                        'done': True
                    }
                else:
                    # Fallback to non-streaming
                    response = self._simple_conversational_response(user_message, session_context, session_id)
                    yield {
                        'content': response.get('response', ''),
                        'status': 'success',
                        'done': True
                    }
                return
            
            # Stream with tools via orchestrator when available
            logger.info(f"✅ Data loaded! Streaming with tools")
            if self.tool_runner and self.orchestrator:
                system_prompt = self._build_system_prompt_refactored(session_context, session_id)
                function_schemas = self.tool_runner.get_function_schemas()
                yield from self.orchestrator.stream_with_tools(
                    self.llm_manager,
                    system_prompt,
                    user_message,
                    function_schemas,
                    session_id,
                    self.tool_runner,
                    interpretation_cb=lambda raw, _msg: self._interpret_raw_output(raw, _msg, session_context, session_id),
                )
            else:
                yield from self._stream_with_tools(user_message, session_context, session_id)
            
        except Exception as e:
            logger.error(f"Error in streaming: {e}")
            yield {
                'content': f'I encountered an issue: {str(e)}',
                'status': 'error',
                'done': True
            }
    
    def _llm_with_tools(self, user_message: str, session_context: Dict, session_id: str) -> Dict[str, Any]:
        """Back-compat shim: delegate to orchestrator + tool_runner when available."""
        system_prompt = self._build_system_prompt_refactored(session_context, session_id)
        if self.tool_runner and self.orchestrator:
            schemas = self.tool_runner.get_function_schemas()
            return self.orchestrator.run_with_tools(
                self.llm_manager,
                system_prompt,
                user_message,
                schemas,
                session_id,
                self.tool_runner,
            )
        # Legacy path
        functions = []
        for tool_name, tool_func in self.tools.items():
            functions.append({
                'name': tool_name,
                'description': tool_func.__doc__ or f"Execute {tool_name}",
                'parameters': self._get_tool_parameters(tool_name)
            })
        response = self.llm_manager.generate_with_functions(
            messages=[{"role": "user", "content": user_message}],
            system_prompt=system_prompt,
            functions=functions,
            temperature=0.7,
            session_id=session_id
        )
        return self._process_llm_response(response, user_message, session_id)
    
    def _stream_with_tools(self, user_message: str, session_context: Dict, session_id: str):
        """Stream LLM response with tools."""
        logger.info(f"🔧 _stream_with_tools called for session {session_id}")
        logger.info(f"📊 Data loaded status: {session_context.get('data_loaded', False)}")
        logger.info(f"📦 Session data cache has session: {session_id in self.session_data}")
        
        # CRITICAL FIX: Ensure data is loaded in session_data before tools are called
        # Load the appropriate dataset based on analysis completion status
        if session_context.get('data_loaded', False) and session_id not in self.session_data:
            try:
                import pandas as pd
                from pathlib import Path
                session_folder = Path(f'instance/uploads/{session_id}')

                # Check if analysis is complete and load appropriate file
                analysis_complete = session_context.get('analysis_complete', False)
                marker_file = session_folder / '.analysis_complete'

                # Prioritize unified dataset if analysis is complete
                if analysis_complete or marker_file.exists():
                    # Try unified dataset first
                    unified_path = session_folder / 'unified_dataset.csv'
                    if unified_path.exists():
                        df = pd.read_csv(unified_path)
                        logger.info(f"✅ Loaded unified dataset for tools: {df.shape} from {unified_path}")
                    else:
                        # Fall back to raw data if unified not found
                        raw_data_path = session_folder / 'raw_data.csv'
                        if raw_data_path.exists():
                            df = pd.read_csv(raw_data_path)
                            logger.info(f"⚠️ Analysis complete but unified dataset not found, loaded raw data: {df.shape}")
                else:
                    # Load raw data for pre-analysis stage
                    raw_data_path = session_folder / 'raw_data.csv'
                    if raw_data_path.exists():
                        df = pd.read_csv(raw_data_path)
                        logger.info(f"✅ Loaded raw data for tools: {df.shape} from {raw_data_path}")

                if 'df' in locals():
                    self.session_data[session_id] = {
                        'data': df,
                        'columns': list(df.columns),
                        'shape': df.shape
                    }
                    logger.info(f"📋 Columns loaded: {list(df.columns)[:5]}...")
            except Exception as e:
                logger.error(f"Failed to load data for tools: {e}")
        
        # Check if LLM manager is available
        if self.llm_manager is None:
            logger.error("LLM manager is not initialized for streaming")
            yield {
                'content': "I'm having trouble connecting to the language model. Please try again in a moment.",
                'status': 'error',
                'done': True
            }
            return
        
        system_prompt = self._build_system_prompt_refactored(session_context, session_id)
        
        functions = []
        for tool_name, tool_func in self.tools.items():
            func_def = {
                'name': tool_name,
                'description': tool_func.__doc__ or f"Execute {tool_name}",
                'parameters': self._get_tool_parameters(tool_name)
            }
            functions.append(func_def)
        
        logger.info(f"🛠️ Passing {len(functions)} tools to LLM for streaming")
        logger.info(f"📝 Tool names: {[f['name'] for f in functions[:3]]}...")
        logger.info(f"🎯 User message: {user_message[:100]}...")
        
        # Track accumulated content for conversation storage
        accumulated_content = []
        
        for chunk in self.llm_manager.generate_with_functions_streaming(
            messages=[{"role": "user", "content": user_message}],
            system_prompt=system_prompt,
            functions=functions,
            temperature=0.7,
            session_id=session_id
        ):
            if chunk.get('function_call'):
                # Execute tool
                function_name = chunk['function_call']['name']
                # Tool call detected - log quietly
                logger.info(f"🔧 Tool call detected: {function_name} with args: {chunk['function_call']['arguments'][:100]}...")

                # BROWSER CONSOLE DEBUG: Send tool selection info to frontend
                debug_msg = f"🔧 TOOL SELECTED: {function_name}"
                logger.info(debug_msg)
                yield {
                    "content": f"<!-- DEBUG: {debug_msg} -->",
                    "status": "success",
                    "debug_tool_call": {
                        "tool_selected": function_name,
                        "tool_args": chunk["function_call"]["arguments"][:200] if chunk["function_call"].get("arguments") else "{}"
                    },
                    "done": False
                }

                # CRITICAL FIX: Use tool_runner for fuzzy name matching instead of direct dict lookup
                # This allows OpenAI to call tools with variations like "runmalariariskanalysis"
                # which will match "run_malaria_risk_analysis" via fuzzy matching
                if self.tool_runner:
                    try:
                        args_str = chunk['function_call']['arguments'] or '{}'
                        logger.info(f"🎯 Executing via tool_runner: {function_name}")
                        result_dict = self.tool_runner.execute(function_name, args_str)

                        # tool_runner.execute returns normalized dict with response/status/tools_used
                        # Convert to format expected by streaming code
                        if result_dict.get('status') == 'error':
                            logger.error(f"❌ Tool execution error: {result_dict.get('response')}")
                            yield {
                                'content': result_dict.get('response', 'Tool execution failed'),
                                'status': 'error',
                                'done': True
                            }
                            return

                        # Success - convert normalized result back to expected format
                        result = {
                            'response': result_dict.get('response', ''),
                            'status': result_dict.get('status', 'success'),
                            'visualizations': result_dict.get('visualizations', []),
                            'download_links': result_dict.get('download_links', []),
                            'tools_used': result_dict.get('tools_used', [function_name])
                        }
                    except Exception as e:
                        logger.error(f"❌ Tool runner execution failed: {e}")
                        yield {
                            'content': f'Tool execution error: {str(e)}',
                            'status': 'error',
                            'done': True
                        }
                        return

                elif function_name in self.tools:
                    try:
                        # Fallback to legacy direct execution if tool_runner not available
                        args_str = chunk['function_call']['arguments'] or '{}'
                        args = json.loads(args_str) if args_str.strip() else {}
                        args['session_id'] = session_id  # Ensure session_id is included

                        logger.debug(f"Executing tool (legacy): {function_name} with args: {args}")

                        # Debug logging to understand parameter issues
                        if function_name == 'execute_sql_query' and 'query' not in args:
                            logger.error(f"SQL query parameter missing! Args: {args}")
                            # Don't add fallback - let it fail properly

                        if function_name == 'create_variable_distribution' and 'variable_name' not in args:
                            logger.error(f"variable_name parameter missing! Args: {args}")
                            # Don't add fallback - let it fail properly

                        result = self.tools[function_name](**args)
                        logger.debug(f"Tool {function_name} completed")
                        
                        ######################## NEW: AUTOMATIC INTERPRETATION ########################
                        # If the tool returned data (either as a structured dict or raw string),
                        # automatically ask the LLM to interpret the results before finalising
                        # the streaming response. This prevents raw, unexplained outputs from
                        # reaching the user.
                        #############################################################################

                        # Flag to toggle interpretation (easy to disable if needed)
                        ENABLE_INTERPRETATION = True

                        def _yield_interpretation(raw_output: str, tools_used_list: list):
                            """Utility to generate and yield interpretation chunks."""
                            if not ENABLE_INTERPRETATION:
                                yield {'content': '', 'status': 'success', 'tools_used': tools_used_list, 'done': True}
                                return
                            try:
                                interpretation = self._interpret_raw_output(
                                    raw_output=raw_output,
                                    user_message=user_message,
                                    session_context=session_context,
                                    session_id=session_id
                                )
                                if interpretation:
                                    # Add proper spacing and section header for clarity
                                    formatted_interpretation = f"\n\n**Analysis:**\n{interpretation}"
                                    yield {
                                        'content': formatted_interpretation,
                                        'status': 'success',
                                        'tools_used': tools_used_list,
                                        'done': True
                                    }
                                else:
                                    yield {'content': '', 'status': 'success', 'tools_used': tools_used_list, 'done': True}
                            except Exception as interp_err:
                                logger.error(f"Error during interpretation: {interp_err}")
                                yield {
                                    'content': f"\n\n⚠️ Interpretation failed: {interp_err}",
                                    'status': 'error',
                                    'tools_used': tools_used_list,
                                    'done': True
                                }

                        # Handle structured dict response
                        if isinstance(result, dict) and 'response' in result:
                            tools_list = result.get('tools_used', [function_name])
                            yield {
                                'content': result['response'],
                                'status': result.get('status', 'success'),
                                'visualizations': result.get('visualizations', []),
                                'download_links': result.get('download_links', []),
                                'tools_used': tools_list,
                                'done': False
                            }
                            yield from _yield_interpretation(result['response'], tools_list)
                            self._store_conversation(session_id, user_message, result['response'])
                            return

                        # Handle raw string response OR ToolExecutionResult
                        else:
                            # Check if it's a ToolExecutionResult with visualization data
                            visualizations = []
                            if hasattr(result, 'data') and result.data and 'web_path' in result.data:
                                # Extract visualization information from ToolExecutionResult
                                viz_data = {
                                    'type': result.data.get('map_type', result.data.get('chart_type', 'visualization')),
                                    'path': result.data.get('web_path', ''),
                                    'url': result.data.get('web_path', ''),
                                    'file_path': result.data.get('file_path', ''),
                                    'title': result.data.get('title', 'Visualization')
                                }
                                visualizations = [viz_data]
                                raw_output = result.message if hasattr(result, 'message') else str(result)
                            elif isinstance(result, dict) and result.get('data') and result['data'].get('web_path'):
                                # Handle dict format with visualization data
                                viz_data = {
                                    'type': result['data'].get('map_type', result['data'].get('chart_type', 'visualization')),
                                    'path': result['data'].get('web_path', ''),
                                    'url': result['data'].get('web_path', ''),
                                    'file_path': result['data'].get('file_path', ''),
                                    'title': result.get('message', 'Visualization')
                                }
                                visualizations = [viz_data]
                                raw_output = result.get('message', str(result))
                            else:
                                raw_output = result if isinstance(result, str) else str(result)
                            
                            tools_list = [function_name]
                            yield {
                                'content': raw_output,
                                'status': 'success',
                                'visualizations': visualizations,
                                'tools_used': tools_list,
                                'done': False
                            }
                            yield from _yield_interpretation(raw_output, tools_list)
                            self._store_conversation(session_id, user_message, raw_output)
                            return
                        # ---------------------------------------------------------------------------
                    except Exception as e:
                        yield {
                            'content': f"Error executing {function_name}: {str(e)}",
                            'status': 'error',
                            'done': True
                        }
                        return
                else:
                    # Tool not found in either tool_runner or self.tools
                    logger.error(f"❌ Unknown function requested: {function_name}")
                    logger.error(f"   Available in self.tools: {list(self.tools.keys())[:5]}...")
                    if self.tool_runner:
                        available_tools = self.tool_runner.registry.list_tools()
                        logger.error(f"   Available in tool_runner: {available_tools[:5] if available_tools else 'None'}...")
                    yield {
                        'content': f"I don't have access to the function '{function_name}'. This might be a configuration issue. Please contact support.",
                        'status': 'error',
                        'done': True
                    }
                    return
            
            # Handle text chunks from streaming (OpenAI format has no 'type' field)
            content = chunk.get('content', '')
            if content:  # Only process if there's actual content
                accumulated_content.append(content)
                yield {
                    'content': content,
                    'status': 'success',
                    'done': False
                }
            elif chunk.get('done'):
                content = chunk.get('content', '')
                if content:
                    accumulated_content.append(content)
                yield {
                    'content': content,
                    'status': 'success',
                    'done': True
                }
                # Store the full conversation
                full_response = ''.join(accumulated_content)
                self._store_conversation(session_id, user_message, full_response)
                return
            else:
                # Handle other chunk types
                content = chunk.get('content', '')
                if content:
                    accumulated_content.append(content)
                yield {
                    'content': content,
                    'status': 'success',
                    'done': False
                }
        
        # If we got here without a done signal, send one
        if accumulated_content:
            yield {
                'content': '',
                'status': 'success',
                'done': True
            }
            full_response = ''.join(accumulated_content)
            self._store_conversation(session_id, user_message, full_response)
    
    def _process_llm_response(self, response: Dict, user_message: str, session_id: str) -> Dict[str, Any]:
        """Refactored: delegate function_call execution to ToolRunner."""
        func_call = response.get('function_call') or response.get('tool_call')
        if func_call and func_call.get('name'):
            if self.tool_runner:
                return self.tool_runner.execute(func_call['name'], func_call.get('arguments', '{}'))
        return {'response': response.get('content', 'No response'), 'tools_used': [], 'status': 'success'}
    
    # Tool Functions - These are the actual functions registered as tools
    def _run_malaria_risk_analysis(self, session_id: str, variables: Optional[List[str]] = None):
        """Run complete dual-method malaria risk analysis (composite scoring + PCA).
        Use ONLY when analysis has NOT been run yet. DO NOT use if analysis is already complete.
        For ITN planning after analysis, use run_itn_planning instead."""
        logger.info("⚡ TOOL: _run_malaria_risk_analysis called")
        logger.info(f"  🆔 Session ID: {session_id}")
        logger.info(f"  📊 Variables: {variables}")
        try:
            # Use the tool directly to get the comprehensive summary
            from app.tools.complete_analysis_tools import RunMalariaRiskAnalysis
            tool = RunMalariaRiskAnalysis()
            
            # Execute the tool with proper parameters
            tool_result = tool.execute(
                session_id=session_id,
                composite_variables=variables,  # Use same variables for both methods if provided
                pca_variables=variables
            )
            
            # Update session to mark analysis as complete on success
            if tool_result.success:
                try:
                    from flask import session
                    session['analysis_complete'] = True
                    session.modified = True
                except:
                    # If not in request context, update conversation history
                    if session_id not in self.conversation_history:
                        self.conversation_history[session_id] = []
                    self.conversation_history[session_id].append({
                        'analysis_complete': True
                    })
            
            # Get the comprehensive message from tool result
            message = tool_result.message  # This contains the custom summary
            
            # Auto-explain any visualizations
            visualizations = tool_result.data.get('visualizations', []) if tool_result.data else []
            if visualizations:
                explanations = []
                for viz in visualizations:
                    if viz.get('file_path'):
                        explanation = self._explain_visualization_universally(
                            viz['file_path'], viz.get('type', 'visualization'), session_id
                        )
                        explanations.append(explanation)
                if explanations:
                    message += "\n\n" + "\n\n".join(explanations)

            # Return structured response to bypass interpretation layer
            hint = "\n\n_Shortcut: type **run malaria risk analysis** to rerun this workflow quickly._"
            return {
                'response': message + hint,
                'status': 'success' if tool_result.success else 'error',
                'tools_used': ['run_malaria_risk_analysis'],
                'visualizations': visualizations
            }
        except Exception as e:
            return f"Error running complete analysis: {str(e)}"
    
    def _run_composite_analysis(self, session_id: str, variables: Optional[List[str]] = None):
        """Run composite scoring malaria risk analysis with equal weights."""
        try:
            result = self.analysis_service.run_composite_analysis(session_id, variables=variables)
            
            if isinstance(result, dict) and result.get('status') == 'success':
                # Update session to mark analysis as complete
                try:
                    from flask import session
                    session['analysis_complete'] = True
                    session.modified = True
                except:
                    # If not in request context, update conversation history
                    if session_id not in self.conversation_history:
                        self.conversation_history[session_id] = []
                    self.conversation_history[session_id].append({
                        'analysis_complete': True
                    })
                
                # Run PCA analysis too for comprehensive comparison
                pca_result = self.analysis_service.run_pca_analysis(session_id, variables=variables)
                
                # Use your proper summary function
                try:
                    from app.tools.complete_analysis_tools import RunMalariaRiskAnalysis
                    
                    analysis_tool = RunMalariaRiskAnalysis()
                    summary = analysis_tool._generate_comprehensive_summary(
                        result, pca_result, {}, 0.0, session_id
                    )
                    
                    return summary
                        
                except Exception as summary_error:
                    logger.error(f"Error calling _generate_summary_from_analysis_results: {summary_error}")
                    logger.error(f"Composite result structure: {result.keys() if result else 'None'}")
                    logger.error(f"PCA result structure: {pca_result.keys() if pca_result else 'None'}")
                    import traceback
                    logger.error(f"Full traceback: {traceback.format_exc()}")
                    return "✅ Composite analysis completed successfully. Results are available - please ask for detailed rankings."
            else:
                # Use your existing error formatter
                from app.services.response_formatter import response_formatter
                return response_formatter.format_error_message(
                    result.get('message', 'Composite analysis failed'),
                    'composite_analysis',
                    ['Check data quality', 'Verify variable selection', 'Review analysis parameters']
                )
        except Exception as e:
            from app.services.response_formatter import response_formatter
            return response_formatter.format_error_message(
                str(e),
                'composite_analysis_execution',
                ['Check data upload', 'Verify system configuration', 'Review error logs']
            )
    
    def _run_pca_analysis(self, session_id: str, variables: Optional[List[str]] = None):
        """Run PCA malaria risk analysis."""
        try:
            result = self.analysis_service.run_pca_analysis(session_id, variables=variables)
            
            # Update session to mark analysis as complete on success
            if result.get('status') == 'success':
                try:
                    from flask import session
                    session['analysis_complete'] = True
                    session.modified = True
                except:
                    # If not in request context, update conversation history
                    if session_id not in self.conversation_history:
                        self.conversation_history[session_id] = []
                    self.conversation_history[session_id].append({
                        'analysis_complete': True
                    })
            
            # Format PCA results properly
            from app.services.response_formatter import response_formatter
            
            if isinstance(result, dict):
                formatted_result = response_formatter.format_analysis_result(result, 'pca')
                return formatted_result
            else:
                return result.get('message', 'PCA analysis completed successfully')
        except Exception as e:
            return f"Error running PCA analysis: {str(e)}"
    
    def _create_vulnerability_map(self, session_id: str, method: str = None):
        """Create vulnerability/risk assessment choropleth map showing ward risk rankings from completed analysis.

        IMPORTANT: Use this tool ONLY when user explicitly requests:
        - "vulnerability map"
        - "risk map"
        - "create vulnerability map comparison"
        - "show vulnerability assessment"

        DO NOT use for mapping raw data variables (use create_variable_distribution for that).

        This requires completed malaria risk analysis with composite_rank or pca_rank columns.
        If method is not specified, creates a side-by-side comparison of both methods.
        If method is specified ('composite' or 'pca'), creates a single map for that method.
        """
        try:
            # If no method specified, use the comparison tool
            if method is None:
                # Use the new comparison tool from the tool registry
                from app.core.tool_registry import get_tool_registry
                tool_registry = get_tool_registry()
                
                # Execute the comparison tool
                result = tool_registry.execute_tool(
                    'create_vulnerability_map_comparison',  # This tool overrides get_tool_name()
                    session_id=session_id,
                    include_statistics=True
                )
                
                if result.get('status') == 'success':
                    # Convert tool result to expected format
                    return {
                        'response': result.get('message', 'Created side-by-side vulnerability map comparison'),
                        'visualizations': [{
                            'type': 'vulnerability_comparison',
                            'file_path': result.get('data', {}).get('file_path', ''),
                            'path': result.get('data', {}).get('web_path', ''),
                            'url': result.get('data', {}).get('web_path', ''),
                            'title': "Vulnerability Assessment Comparison",
                            'description': "Side-by-side comparison of Composite and PCA vulnerability methods"
                        }],
                        'tools_used': ['create_vulnerability_map_comparison'],
                        'status': 'success'
                    }
                else:
                    return f"Error creating vulnerability comparison: {result.get('message', 'Unknown error')}"
            
            # Otherwise use the specific method requested
            else:
                result = self.visualization_service.create_vulnerability_map(session_id, method=method)
                message = result.get('message', f'Vulnerability map created using {method} method')

                # Auto-explain the visualization if file_path exists
                if result.get('file_path'):
                    explanation = self._explain_visualization_universally(
                        result['file_path'], 'vulnerability_map', session_id
                    )
                    message += f"\\n\\n{explanation}"

                # Return structured response if successful
                if result.get('status') == 'success' and result.get('web_path'):
                    return {
                        'response': message,
                        'visualizations': [{
                            'type': result.get('visualization_type', 'vulnerability_map'),
                            'file_path': result.get('file_path', ''),
                            'path': result.get('web_path', ''),
                            'url': result.get('web_path', ''),
                            'title': f"Vulnerability Map ({method.title()} Method)",
                            'description': f"Ward vulnerability classification using {method} method"
                        }],
                        'tools_used': ['create_vulnerability_map'],
                        'status': 'success'
                    }
                else:
                    return f"Error creating vulnerability map: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating vulnerability map: {str(e)}"
    
    def _create_box_plot(self, session_id: str, method: str = 'composite'):
        """Create box plots showing vulnerability score distributions."""
        try:
            result = self.visualization_service.create_box_plot(session_id, method=method)
            message = result.get('message', f'Box plot created for {method} scores')
            
            # Auto-explain the visualization if file_path exists
            if result.get('file_path'):
                explanation = self._explain_visualization_universally(
                    result['file_path'], 'box_plot', session_id
                )
                message += f"\\n\\n{explanation}"
            
            # Return structured response if successful
            if result.get('status') == 'success' and result.get('web_path'):
                return {
                    'response': message,
                    'visualizations': [{
                        'type': result.get('visualization_type', 'box_plot'),
                        'file_path': result.get('file_path', ''),
                        'path': result.get('web_path', ''),
                        'url': result.get('web_path', ''),
                        'title': f"Box Plot ({method.title()} Method)",
                        'description': f"Vulnerability score distribution using {method} method"
                    }],
                    'tools_used': ['create_box_plot'],
                    'status': 'success'
                }
            else:
                return f"Error creating box plot: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating box plot: {str(e)}"
    
    def _create_pca_map(self, session_id: str):
        """Create PCA-based vulnerability map."""
        try:
            result = self.visualization_service.create_pca_map(session_id)
            message = result.get('message', 'PCA vulnerability map created')
            
            # Auto-explain the visualization if file_path exists
            if result.get('file_path'):
                explanation = self._explain_visualization_universally(
                    result['file_path'], 'pca_map', session_id
                )
                message += f"\\n\\n{explanation}"
            
            # Return structured response if successful
            if result.get('status') == 'success' and result.get('web_path'):
                return {
                    'response': message,
                    'visualizations': [{
                        'type': result.get('visualization_type', 'pca_map'),
                        'file_path': result.get('file_path', ''),
                        'path': result.get('web_path', ''),
                        'url': result.get('web_path', ''),
                        'title': "PCA Vulnerability Map",
                        'description': "Ward vulnerability classification using PCA method"
                    }],
                    'tools_used': ['create_pca_map'],
                    'status': 'success'
                }
            else:
                return f"Error creating PCA map: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating PCA map: {str(e)}"
    
    def _create_variable_distribution(self, session_id: str, variable_name: str):
        """Create an interactive choropleth map showing the spatial distribution of a RAW DATA COLUMN.

        Use this ONLY for mapping existing columns from uploaded data (TPR, rainfall, elevation, etc).
        DO NOT use for vulnerability/risk maps (use create_vulnerability_map for that).

        REQUIRES: 'variable_name' parameter with the exact column name to visualize.

        Examples:
        - User: "plot the map distribution for mean_rainfall" -> Use variable_name='mean_rainfall'
        - User: "show pfpr on map" -> Use variable_name='pfpr'
        - User: "map TPR distribution" -> Use variable_name='TPR'

        DO NOT use for:
        - "vulnerability map" (use create_vulnerability_map)
        - "risk map" (use create_vulnerability_map)
        """
        try:
            from app.tools.variable_distribution import VariableDistribution
            
            # Create the tool instance
            tool = VariableDistribution(variable_name=variable_name)
            
            # Execute the tool
            result = tool.execute(session_id)
            
            if result.success:
                message = result.message
                
                # Auto-explain the visualization if it was created
                if result.data and result.data.get('file_path'):
                    explanation = self._explain_visualization_universally(
                        result.data['file_path'], 'variable_distribution', session_id
                    )
                    message += f"\n\n{explanation}"
                
                # Return structured response with visualization data
                return {
                    'response': message,
                    'visualizations': [{
                        'type': result.data.get('chart_type', 'variable_distribution'),
                        'file_path': result.data.get('file_path', ''),
                        'path': result.data.get('web_path', ''),  # Frontend expects 'path' key
                        'url': result.data.get('web_path', ''),   # Also provide 'url' for compatibility
                        'title': f"{result.data.get('variable', 'Variable')} Distribution",
                        'description': f"Spatial distribution of {result.data.get('variable', 'variable')} across study area"
                    }] if result.data else [],
                    'tools_used': ['create_variable_distribution'],
                    'status': 'success'
                }
            else:
                return f"Error creating variable distribution: {result.message}"
        except Exception as e:
            return f"Error creating variable distribution: {str(e)}"
    
    def _execute_data_query(self, session_id: str, query: str, code: Optional[str] = None):
        """Execute complex data analysis using Python code. Use for statistics, correlations, or advanced analysis.
        The 'query' parameter is REQUIRED and should describe what analysis to perform.
        Examples: query='check data quality', query='correlation between rainfall and malaria', query='statistical summary'"""
        try:
            # Check if data is available via data service first
            data_handler = self.data_service.get_handler(session_id)
            if not data_handler or not hasattr(data_handler, 'csv_data') or data_handler.csv_data is None:
                return "Error executing query: No data available. Please upload data first."
            
            # Initialize conversational data access for this session
            # Always create a new instance to ensure proper session context
            logger.info(f"Creating ConversationalDataAccess for session: {session_id}")
            from app.services.conversational_data_access import ConversationalDataAccess
            conversational_data_access = ConversationalDataAccess(session_id, self.llm_manager)
            
            if code:
                logger.info(f"Executing provided code: {code}")
                result = conversational_data_access.execute_code(code)
            else:
                logger.info(f"Processing query: {query}")
                result = conversational_data_access.process_query(query)
            
            if result.get('success'):
                # Get the formatted output from the executed code
                output = result.get('output', '').strip()
                plot_data = result.get('plot_data')
                
                # Return structured response with visualizations
                response_data = {
                    'response': output if output else f"Query executed successfully: {query}",
                    'visualizations': [],
                    'tools_used': ['execute_data_query'],
                    'status': 'success'
                }
                
                # Add visualization if plot was generated
                if plot_data:
                    # Determine specific chart type from query context
                    viz_type = self._determine_chart_type_from_query(query)
                    response_data['visualizations'].append({
                        'type': viz_type,
                        'data': plot_data,
                        'title': f'Analysis Visualization - {query[:50]}...' if len(query) > 50 else f'Analysis Visualization - {query}'
                    })
                
                return response_data
            else:
                error_msg = f"Error executing query: {result.get('error', 'Unknown error')}"
                return error_msg
        except Exception as e:
            return f"Error in data query: {str(e)}"
    
    def _execute_sql_query(self, session_id: str, query: str):
        """Execute SQL queries on the dataset.
        REQUIRES: 'query' parameter with a valid SQL string. The table name is always 'df'.
        Use this when users ask to see data, list columns, or filter records.
        User asks: 'what are the variables in my data?' -> Use query: 'SELECT * FROM df LIMIT 1'
        User asks: 'show top 5 wards by pfpr' -> Use query: 'SELECT * FROM df ORDER BY pfpr DESC LIMIT 5'"""
        try:
            # For now, continue using the conversational data access
            # The interpretation will happen in the streaming handler
            logger.info(f"Executing SQL query: {query}")
            from app.services.conversational_data_access import ConversationalDataAccess
            conversational_data_access = ConversationalDataAccess(session_id, self.llm_manager)

            # Use the process_sql_query method which handles all stages properly
            result = conversational_data_access.process_sql_query(query, original_query=query)
            
            if result.get('success'):
                return result.get('output', 'Query executed successfully')
            else:
                return f"Error executing SQL: {result.get('error', 'Unknown error')}"
        except Exception as e:
            logger.error(f"SQL query error: {e}")
            return f"Error executing SQL query: {str(e)}"
    
    def _determine_chart_type_from_query(self, query: str) -> str:
        """Determine specific chart type from user query to avoid visualization conflicts."""
        query_lower = query.lower()
        
        # Check for specific chart types mentioned in the query
        if 'scatter' in query_lower:
            return 'scatter_plot'
        elif 'histogram' in query_lower or 'hist' in query_lower:
            return 'histogram'
        elif ('distribution' in query_lower and 'plot' in query_lower) or 'distplot' in query_lower:
            return 'histogram'  # Most distribution plots are histograms
        elif 'box plot' in query_lower or 'boxplot' in query_lower:
            return 'box_plot'
        elif 'bar chart' in query_lower or 'bar plot' in query_lower or 'barplot' in query_lower:
            return 'bar_chart'
        elif 'line chart' in query_lower or 'line plot' in query_lower or 'lineplot' in query_lower:
            return 'line_plot'
        elif 'pie chart' in query_lower or 'pie plot' in query_lower:
            return 'pie_chart'
        elif 'heatmap' in query_lower or 'heat map' in query_lower:
            return 'heatmap'
        elif 'density' in query_lower and 'plot' in query_lower:
            return 'density_plot'
        elif 'violin' in query_lower:
            return 'violin_plot'
        elif 'correlation' in query_lower and 'plot' in query_lower:
            return 'scatter_plot'  # Correlation plots are usually scatter plots
        else:
            # Return a unique type for each general plot to prevent conflicts
            import time
            return f'conversational_plot_{int(time.time())}'
    
    def _explain_analysis_methodology(self, session_id: str, method: str = 'both'):
        """Explain how malaria risk analysis methodologies work (composite scoring, PCA, or both)."""
        try:
            # Use LLM to generate methodology explanation
            if method == 'composite':
                explanation = """**Composite Scoring Methodology**

Composite scoring combines multiple malaria risk indicators into a single vulnerability score:

1. **Variable Selection**: Selects scientifically-validated variables based on Nigerian geopolitical zones
2. **Normalization**: Standardizes all variables to 0-1 scale for fair comparison
3. **Equal Weighting**: All variables contribute equally to the final score
4. **Aggregation**: Sums normalized values to create composite vulnerability score
5. **Ranking**: Ranks wards from highest to lowest risk for intervention targeting

This method is transparent, interpretable, and follows WHO guidelines for malaria stratification."""
            
            elif method == 'pca':
                explanation = """**Principal Component Analysis (PCA) Methodology**

PCA reduces dimensionality while preserving variance in malaria risk data:

1. **Data Standardization**: Centers and scales all variables
2. **Covariance Analysis**: Identifies relationships between variables
3. **Component Extraction**: Finds principal components that explain maximum variance
4. **Weight Calculation**: Automatically determines variable importance
5. **Score Generation**: Creates data-driven vulnerability scores
6. **Interpretation**: First component typically captures overall malaria risk

This method is statistically robust and reveals hidden patterns in the data."""
            
            else:  # both
                explanation = """**Dual-Method Malaria Risk Analysis**

ChatMRPT uses both composite scoring and PCA for comprehensive assessment:

**Composite Scoring**: Transparent, equal-weighted approach following WHO guidelines
- Pros: Interpretable, policy-friendly, scientifically grounded
- Use when: Clear intervention priorities needed

**PCA Analysis**: Statistical approach revealing data patterns
- Pros: Objective, captures complex relationships, statistically robust
- Use when: Exploring underlying risk structures

**Comparison**: Both methods often agree on high-risk areas but may differ in rankings. Use both for robust decision-making and to validate findings."""
            
            return explanation
        except Exception as e:
            return f"Error explaining methodology: {str(e)}"
    
    def _create_settlement_map(self, session_id: str, ward_name: str = None, zoom_level: int = 11, **kwargs):
        """Create interactive settlement map showing building types for specific wards."""
        try:
            # Filter out any invalid parameters that LLM might pass
            valid_params = {'session_id': session_id}
            if ward_name:
                valid_params['ward_name'] = ward_name
            if zoom_level:
                valid_params['zoom_level'] = zoom_level
            
            from app.tools.settlement_visualization_tools import create_settlement_map
            
            result = create_settlement_map(**valid_params)
            
            if result.get('status') == 'success':
                message = result.get('message', f'Settlement map created')
                
                # Add ward-specific information
                if ward_name:
                    message += f" for {ward_name} ward"
                
                # Auto-explain the visualization if file_path exists
                if result.get('file_path'):
                    explanation = self._explain_visualization_universally(
                        result['file_path'], 'settlement_map', session_id
                    )
                    message += f"\n\n{explanation}"
                
                # Return structured response
                if result.get('web_path'):
                    return {
                        'response': message,
                        'visualizations': [{
                            'type': 'settlement_map',
                            'file_path': result.get('file_path', ''),
                            'path': result.get('web_path', ''),
                            'url': result.get('web_path', ''),
                            'title': f"Settlement Map{' - ' + ward_name if ward_name else ''}",
                            'description': f"Building classification map showing settlement types{' for ' + ward_name + ' ward' if ward_name else ''}"
                        }],
                        'tools_used': ['create_settlement_map'],
                        'status': 'success'
                    }
                else:
                    return message
            else:
                return f"Error creating settlement map: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating settlement map: {str(e)}"
    
    def _show_settlement_statistics(self, session_id: str):
        """Show comprehensive statistics about available settlement data."""
        try:
            from app.tools.settlement_visualization_tools import show_settlement_statistics
            
            result = show_settlement_statistics(session_id)
            
            if result.get('status') == 'success':
                message = result.get('message', 'Settlement statistics retrieved')
                
                # Enhance with AI explanation if available
                if result.get('ai_response'):
                    message = result['ai_response']
                
                return {
                    'response': message,
                    'tools_used': ['show_settlement_statistics'],
                    'status': 'success',
                    'data': result.get('data', {})
                }
            else:
                return f"Error getting settlement statistics: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error getting settlement statistics: {str(e)}"

    def _create_urban_extent_map(self, session_id: str, threshold: float = 30.0):
        """Create urban extent classification map showing urban vs rural areas."""
        try:
            from app.core.tool_registry import get_tool_registry
            tool_registry = get_tool_registry()

            result = tool_registry.execute_tool(
                'createurbanextentmap',
                session_id=session_id,
                urban_threshold=threshold
            )

            if result.get('status') == 'success':
                return {
                    'response': result.get('message', 'Urban extent map created'),
                    'visualizations': [{
                        'type': 'urban_extent_map',
                        'file_path': result.get('data', {}).get('file_path', ''),
                        'path': result.get('data', {}).get('web_path', ''),
                        'url': result.get('data', {}).get('web_path', ''),
                        'title': 'Urban Extent Classification',
                        'description': f'Urban areas (>{threshold}% built-up) vs rural areas'
                    }],
                    'tools_used': ['createurbanextentmap'],
                    'status': 'success'
                }
            else:
                return f"Error creating urban extent map: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating urban extent map: {str(e)}"

    def _create_decision_tree(self, session_id: str):
        """Create decision tree visualization showing risk factor logic."""
        try:
            from app.core.tool_registry import get_tool_registry
            tool_registry = get_tool_registry()

            result = tool_registry.execute_tool(
                'createdecisiontree',
                session_id=session_id
            )

            if result.get('status') == 'success':
                return {
                    'response': result.get('message', 'Decision tree visualization created'),
                    'visualizations': [{
                        'type': 'decision_tree',
                        'file_path': result.get('data', {}).get('file_path', ''),
                        'path': result.get('data', {}).get('web_path', ''),
                        'url': result.get('data', {}).get('web_path', ''),
                        'title': 'Risk Factor Decision Tree',
                        'description': 'Decision logic for malaria risk assessment'
                    }],
                    'tools_used': ['createdecisiontree'],
                    'status': 'success'
                }
            else:
                return f"Error creating decision tree: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating decision tree: {str(e)}"


    def _create_composite_score_maps(self, session_id: str):
        """Create composite score maps with individual model breakdowns."""
        try:
            from app.core.tool_registry import get_tool_registry
            tool_registry = get_tool_registry()

            result = tool_registry.execute_tool(
                'createcompositescoremaps',
                session_id=session_id
            )

            if result.get('status') == 'success':
                return {
                    'response': result.get('message', 'Composite score maps created'),
                    'visualizations': [{
                        'type': 'composite_score_maps',
                        'file_path': result.get('data', {}).get('file_path', ''),
                        'path': result.get('data', {}).get('web_path', ''),
                        'url': result.get('data', {}).get('web_path', ''),
                        'title': 'Composite Score Model Breakdown',
                        'description': 'Individual model contributions to composite malaria risk score'
                    }],
                    'tools_used': ['createcompositescoremaps'],
                    'status': 'success'
                }
            else:
                return f"Error creating composite score maps: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating composite score maps: {str(e)}"

    def _create_composite_vulnerability_map(self, session_id: str):
        """Create vulnerability map specifically for composite method."""
        try:
            from app.core.tool_registry import get_tool_registry
            tool_registry = get_tool_registry()

            result = tool_registry.execute_tool(
                'createcompositevulnerabilitymap',  # lowercase tool name
                session_id=session_id
            )

            if result.get('status') == 'success':
                return {
                    'response': result.get('message', 'Composite vulnerability map created'),
                    'visualizations': [{
                        'type': 'composite_vulnerability_map',
                        'file_path': result.get('data', {}).get('file_path', ''),
                        'path': result.get('data', {}).get('web_path', ''),
                        'url': result.get('data', {}).get('web_path', ''),
                        'title': 'Composite Vulnerability Map',
                        'description': 'Ward vulnerability classification using composite method'
                    }],
                    'tools_used': ['createcompositevulnerabilitymap'],
                    'status': 'success'
                }
            else:
                return f"Error creating composite vulnerability map: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating composite vulnerability map: {str(e)}"

    def _explain_visualization_universally(self, file_path: str, viz_type: str, session_id: str) -> str:
        """Generate a short explanation for the given visualization.

        This wraps :class:`UniversalVizExplainer` but shields the main request
        flow from failures—if the explainer cannot run we simply return an empty
        string rather than breaking the calling tool pipeline.
        """
        if not file_path:
            return ""

        try:
            if not hasattr(self, "_viz_explainer") or self._viz_explainer is None:
                from app.services.universal_viz_explainer import UniversalVizExplainer

                self._viz_explainer = UniversalVizExplainer()

            explanation = self._viz_explainer.explain_visualization(
                viz_path=file_path,
                viz_type=viz_type,
                session_id=session_id,
            )

            if explanation and explanation.strip():
                return explanation.strip()

        except Exception as exc:  # pragma: no cover - best-effort helper
            logger.warning(
                "Visualization explanation failed for %s (type=%s): %s",
                file_path,
                viz_type,
                exc,
            )

        return ""

    def _get_session_context(self, session_id: str, session_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get session context using SessionContextService which properly loads data files."""
        # Use SessionContextService if available (properly loads CSVs and creates current_data)
        if self.context_service:
            try:
                context = self.context_service.get_context(session_id, session_data)
                logger.info(f"📂 SessionContextService loaded context for {session_id}:")
                logger.info(f"  - Data loaded: {context.get('data_loaded', False)}")
                logger.info(f"  - CSV loaded: {context.get('csv_loaded', False)}")
                logger.info(f"  - Shapefile loaded: {context.get('shapefile_loaded', False)}")
                logger.info(f"  - Analysis complete: {context.get('analysis_complete', False)}")
                logger.info(f"  - Current data: {context.get('current_data', 'None')}")
                logger.info(f"  - Columns: {len(context.get('columns', []))} found")
                return context
            except Exception as e:
                logger.warning(f"SessionContextService failed: {e}, falling back to manual detection")

        # Fallback: manual file checking (legacy behavior)
        from pathlib import Path
        session_folder = Path(f"instance/uploads/{session_id}")
        if session_folder.exists():
            # Check for CSV files in root folder
            has_csv = any(f.suffix in ['.csv', '.xlsx', '.xls'] for f in session_folder.glob('*'))

            # Check for shapefiles - look in both root AND shapefile/ subdirectory
            has_shapefile = any(f.suffix == '.shp' for f in session_folder.glob('*'))
            shapefile_dir = session_folder / 'shapefile'
            if not has_shapefile and shapefile_dir.exists():
                has_shapefile = any(f.suffix == '.shp' for f in shapefile_dir.glob('*'))

            # Check for .zip files containing shapefiles
            if not has_shapefile:
                has_shapefile = any(f.suffix == '.zip' and 'shapefile' in f.name.lower() for f in session_folder.glob('*'))

            analysis_marker = session_folder / '.analysis_complete'

            # Load CSV to extract column names and row count
            columns = []
            current_data = "No data uploaded"
            if has_csv:
                try:
                    import pandas as pd
                    # Try common file patterns in priority order
                    file_patterns = ['unified_dataset.csv', 'raw_data.csv', 'data_analysis.csv', 'uploaded_data.csv']
                    df = None
                    for pattern in file_patterns:
                        csv_path = session_folder / pattern
                        if csv_path.exists():
                            df = pd.read_csv(csv_path)
                            columns = df.columns.tolist()
                            current_data = f"{len(df)} rows"
                            logger.info(f"📋 Loaded {len(df)} rows, {len(columns)} columns from {pattern}")
                            break

                    # Fallback: load any CSV file
                    if not columns:
                        csv_files = list(session_folder.glob('*.csv'))
                        if csv_files and not csv_files[0].name.startswith('.'):
                            df = pd.read_csv(csv_files[0])
                            columns = df.columns.tolist()
                            current_data = f"{len(df)} rows"
                            logger.info(f"📋 Loaded {len(df)} rows, {len(columns)} columns from {csv_files[0].name}")
                except Exception as e:
                    logger.warning(f"Could not load CSV: {e}")
                    current_data = "Data files found but could not be loaded"

            logger.info(f"📂 File-based session detection for {session_id}:")
            logger.info(f"  - CSV files found: {has_csv}")
            logger.info(f"  - Shapefile found: {has_shapefile}")
            logger.info(f"  - Analysis complete: {analysis_marker.exists()}")
            logger.info(f"  - Current data: {current_data}")

            return {
                'data_loaded': has_csv or has_shapefile,
                'csv_loaded': has_csv,
                'shapefile_loaded': has_shapefile,
                'analysis_complete': analysis_marker.exists(),
                'current_data': current_data,
                'state_name': session_data.get('state_name', 'Not specified') if session_data else 'Not specified',
                'columns': columns,
                'session_id': session_id
            }

        logger.warning(f"⚠️ Session folder not found: {session_folder}")
        return {'data_loaded': False, 'state_name': 'Not specified', 'current_data': 'No data uploaded'}

    def _handle_special_workflows(self, user_message: str, session_id: str, session_data: Dict[str, Any] = None, **kwargs) -> Optional[Dict[str, Any]]:
        """Handle special workflows - simplified version."""
        # For now, just return None to let normal flow continue
        # This prevents the error and allows messages to be processed
        return None

    def _simple_conversational_response(self, user_message: str, session_context: Dict, session_id: str) -> Dict:
        """Simple conversational response without tools - now with context awareness."""
        try:
            # Build system prompt with UI context
            system_prompt = self._build_system_prompt_refactored(session_context, session_id)

            # Generate response with system context
            response = self.llm_manager.generate(
                user_message,
                system_prompt=system_prompt,
                temperature=0.7,
                max_tokens=500
            )

            # Store conversation
            self._store_conversation(session_id, user_message, response)

            return {
                'response': response,
                'status': 'success',
                'tools_used': []
            }
        except Exception as e:
            logger.error(f"Error generating conversational response: {e}")
            return {
                'response': "I encountered an error processing your message. Please try again.",
                'status': 'error',
                'tools_used': []
            }

    # NEW: ITN Planning Tool
    def _run_itn_planning(self, session_id: str, total_nets: Optional[int] = 10000, avg_household_size: Optional[float] = 5.0, urban_threshold: Optional[float] = 30.0, method: str = 'composite'):
        """Plan ITN (Insecticide-Treated Net) distribution AFTER analysis is complete.
        Use this tool when user wants to plan ITN distribution, allocate bed nets, or create intervention plans.
        This tool uses existing analysis rankings - DO NOT run analysis again if already complete.
        Keywords: ITN, bed nets, net distribution, intervention planning, allocate nets."""
        logger.info("🛏️ ITN: _run_itn_planning called")
        logger.info(f"  🆔 Session ID: {session_id}")
        logger.info(f"  🔢 Total Nets: {total_nets}")
        logger.info(f"  🏠 Household Size: {avg_household_size}")
        logger.info(f"  🏙️ Urban Threshold: {urban_threshold}")
        logger.info(f"  📊 Method: {method}")
        try:
            # Check if analysis is complete first
            session_context = self._get_session_context(session_id)
            analysis_complete = session_context.get('analysis_complete', False)
            
            # CRITICAL FIX: Also check for physical evidence (marker file)
            if not analysis_complete:
                from pathlib import Path
                marker_file = Path(f"instance/uploads/{session_id}/.analysis_complete")
                if marker_file.exists():
                    analysis_complete = True
                    logger.info(f"✅ Found .analysis_complete marker, overriding flag for ITN planning in {session_id}")
                    
                    # Update state to match evidence
                    try:
                        from app.core.workflow_state_manager import WorkflowStateManager
                        state_manager = WorkflowStateManager(session_id)
                        state_manager.update_state({
                            'analysis_complete': True
                        }, transition_reason="ITN tool found analysis evidence")
                    except Exception as e:
                        logger.warning(f"Could not update state: {e}")
            
            data_handler = self.data_service.get_handler(session_id)
            if not data_handler:
                return 'No data available. Please run analysis first.'
            
            ######################## NEW: DIRECT RANKINGS CHECK ########################
            # Just check the unified dataset - it has everything we need
            if hasattr(data_handler, 'unified_dataset') and data_handler.unified_dataset is not None:
                has_rankings = 'composite_rank' in data_handler.unified_dataset.columns or 'overall_rank' in data_handler.unified_dataset.columns
            else:
                # Try to load unified dataset
                data_handler._load_unified_dataset()
                has_rankings = (data_handler.unified_dataset is not None and 
                               ('composite_rank' in data_handler.unified_dataset.columns or 'overall_rank' in data_handler.unified_dataset.columns))
            
            if has_rankings:
                analysis_complete = True  # Override flag if rankings exist
                logger.info(f"Overrode analysis_complete to True based on unified dataset rankings for session {session_id}")
            ############################################################################
            
            if not analysis_complete:
                return 'Analysis has not been completed yet. Please run the malaria risk analysis first before planning ITN distribution.'
            
            from app.analysis.itn_pipeline import calculate_itn_distribution
            data_handler = self.data_service.get_handler(session_id)
            if not data_handler:
                return 'No data available. Please run analysis first.'
            
            # Check if unified dataset exists (it has all the rankings we need)
            if not hasattr(data_handler, 'unified_dataset') or data_handler.unified_dataset is None:
                # Try to load it
                data_handler._load_unified_dataset()
                if data_handler.unified_dataset is None:
                    return 'Analysis rankings not found. Please run the malaria risk analysis first to generate vulnerability rankings.'
            
            # Use the ITN planning tool to get comprehensive results with download links
            from app.tools.itn_planning_tools import PlanITNDistribution
            
            # Create tool instance with parameters
            tool = PlanITNDistribution(
                total_nets=total_nets if total_nets != 10000 else None,
                avg_household_size=avg_household_size,
                urban_threshold=urban_threshold,
                method=method
            )
            
            # Execute the tool
            tool_result = tool.execute(session_id=session_id)
            
            if not tool_result.success:
                return {
                    'response': tool_result.message,
                    'status': 'error',
                    'tools_used': ['run_itn_planning']
                }
            
            # Extract visualizations and download links from tool result
            visualizations = []
            if tool_result.web_path:
                visualizations.append({
                    'type': 'itn_map',
                    'path': tool_result.web_path,
                    'url': tool_result.web_path
                })
            
            # Return structured response with download links
            return {
                'response': tool_result.message,
                'visualizations': visualizations,
                'download_links': tool_result.download_links if hasattr(tool_result, 'download_links') else [],
                'tools_used': ['run_itn_planning'],
                'status': 'success'
            }
        except Exception as e:
            return f"Error planning ITN: {str(e)}"

    def _analyze_data_with_python(self, session_id: str, query: str) -> Dict[str, Any]:
        """
        Execute custom Python analysis on user data via DataExplorationAgent.

        Use this tool for ALL data analysis queries including:

        STATISTICAL TESTS (scipy.stats):
        - ANOVA tests (f_oneway, kruskal)
        - t-tests (ttest_ind, ttest_rel, mannwhitneyu)
        - Correlation tests (pearsonr, spearmanr)
        - Chi-square tests (chi2_contingency)
        - Normality tests (shapiro, kstest)

        MACHINE LEARNING (sklearn):
        - Clustering (KMeans, DBSCAN, AgglomerativeClustering)
        - Dimensionality reduction (PCA, NMF, t-SNE)
        - Regression (LinearRegression, LogisticRegression, RandomForest)
        - Classification (DecisionTree, SVM, GradientBoosting)
        - Preprocessing (StandardScaler, MinMaxScaler)

        DATA ANALYSIS (pandas, numpy):
        - Filtering, aggregation, groupby operations
        - Custom calculations and transformations
        - Statistical summaries (describe, quantile, etc.)
        - Custom data queries (e.g., "show top 10 wards by population")

        GEOSPATIAL ANALYSIS (geopandas):
        - Spatial joins and overlays
        - Distance calculations
        - Coordinate transformations
        - Geospatial queries

        VISUALIZATIONS (plotly):
        - Interactive charts (scatter, bar, line, heatmap)
        - Statistical plots (box, violin, histogram)
        - Geospatial maps

        Available libraries: pandas, numpy, scipy, sklearn, geopandas, plotly, matplotlib, seaborn

        Args:
            session_id: Session identifier
            query: Natural language description of analysis to perform

        Returns:
            Dict with response, visualizations, tools_used (matching RI format)
        """
        logger.info(f"🐍 TOOL: analyze_data_with_python called")
        logger.info(f"  Session: {session_id}")
        logger.info(f"  Query: {query[:100]}...")

        try:
            # Reuse agent if exists, create if not (for conversation memory)
            if session_id not in self.data_agents:
                from app.data_analysis_v3.core.data_exploration_agent import DataExplorationAgent
                logger.info(f"🆕 Creating new DataExplorationAgent for session {session_id}")
                self.data_agents[session_id] = DataExplorationAgent(session_id=session_id)
            else:
                logger.info(f"♻️ Reusing existing DataExplorationAgent for session {session_id} (conversation memory active)")
            
            # Get the cached agent
            agent = self.data_agents[session_id]

            # Execute query (synchronous interface)
            result = agent.analyze_sync(query)

            # Format result to match RI tool contract
            # CRITICAL: RI expects 'response', agent returns 'message'
            return {
                'status': 'success' if result.get('success', False) else 'error',  # Map boolean 'success' to string 'status'
                'response': result.get('message', ''),  # Map 'message' to 'response'
                'visualizations': result.get('visualizations', []),
                'tools_used': ['analyze_data_with_python']
            }

        except Exception as e:
            logger.error(f"Error in analyze_data_with_python: {e}", exc_info=True)
            return {
                'status': 'error',
                'response': f'I encountered an error analyzing the data: {str(e)}',
                'tools_used': ['analyze_data_with_python']
            }

    # Helper Methods
    def _run_data_quality_check(self, session_id: str):
        """Check data quality including missing values, duplicates, and statistics."""
        try:
            import pandas as pd
            from pathlib import Path
            
            # Load the data
            session_folder = Path(f'instance/uploads/{session_id}')
            raw_data_path = session_folder / 'raw_data.csv'
            
            if not raw_data_path.exists():
                return "No data file found. Please upload data first."
            
            df = pd.read_csv(raw_data_path)
            
            # Calculate statistics
            total_missing = df.isnull().sum().sum()
            duplicates = df.duplicated().sum()
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
            
            # Find malaria-relevant variables
            malaria_vars = []
            env_vars = []
            risk_vars = []
            
            for col in df.columns:
                col_lower = col.lower()
                if 'tpr' in col_lower or 'test' in col_lower or 'positive' in col_lower:
                    malaria_vars.append(col)
                elif any(x in col_lower for x in ['evi', 'ndvi', 'soil', 'rain', 'temp', 'humid']):
                    env_vars.append(col)
                elif any(x in col_lower for x in ['urban', 'housing', 'population', 'density']):
                    risk_vars.append(col)
            
            # Format response
            response = f"""**Data Quality Check Complete**

📊 Your dataset has {total_missing} total missing values (minimal impact on analysis).

✅ **{'No duplicate entries' if duplicates == 0 else f'{duplicates} duplicate entries found'}** - {'each ward has unique data' if duplicates == 0 else 'consider removing duplicates'}.

**Key Dataset Characteristics:**
• Both numeric indicators ({len(numeric_cols)}) and categorical identifiers ({len(categorical_cols)})

**Malaria-Relevant Variables Found:**
• **Health indicators**: {', '.join(malaria_vars[:3]) if malaria_vars else 'None detected'}
• **Environmental factors**: {', '.join(env_vars[:4]) if env_vars else 'None detected'}  
• **Risk modifiers**: {', '.join(risk_vars[:3]) if risk_vars else 'None detected'}

**Analysis Readiness: ✅ Ready**
Your data is suitable for analysis. You can now run comprehensive malaria risk assessment to identify priority wards for intervention.

Would you like me to:
• Run the full malaria risk analysis?
• Explore specific variables in detail?
• Create visualizations of key indicators?"""
            
            return response
            
        except Exception as e:
            return f"Error checking data quality: {str(e)}"

    def _get_tool_parameters(self, tool_name: str) -> Dict[str, Any]:
        """Get parameter schema for a tool."""
        base_params = {
            'type': 'object',
            'properties': {
                'session_id': {'type': 'string', 'description': 'Session identifier'}
            },
            'required': ['session_id']
        }
        
        if 'analysis' in tool_name:
            base_params['properties']['variables'] = {
                'type': 'array',
                'items': {'type': 'string'},
                'description': 'Optional custom variables for analysis. When specified, these will override automatic region-based selection.'
            }
        
        if 'map' in tool_name or 'plot' in tool_name:
            if tool_name == 'create_vulnerability_map':
                # For vulnerability map, method is optional - defaults to side-by-side comparison
                base_params['properties']['method'] = {
                    'type': 'string',
                    'enum': ['composite', 'pca'],
                    'description': 'Analysis method to visualize. If not specified, shows side-by-side comparison of both methods.'
                }
            else:
                base_params['properties']['method'] = {
                    'type': 'string',
                    'enum': ['composite', 'pca'],
                    'description': 'Analysis method to visualize'
                }
        
        if tool_name == 'execute_data_query':
            base_params['properties'].update({
                'query': {'type': 'string', 'description': 'Natural language query about the data'},
                'code': {'type': 'string', 'description': 'Optional Python code to execute'}
            })
            base_params['required'].append('query')
        
        if tool_name == 'execute_sql_query':
            base_params['properties'].update({
                'query': {
                    'type': 'string', 
                    'description': 'The SQL query string to execute on the dataframe. The table is always named "df". This parameter is REQUIRED and must contain a valid SQL query.',
                }
            })
            base_params['required'].append('query')
        
        if tool_name == 'explain_analysis_methodology':
            base_params['properties']['method'] = {
                'type': 'string',
                'enum': ['composite', 'pca', 'both'],
                'description': 'Which methodology to explain'
            }
        
        if tool_name == 'create_variable_distribution':
            base_params['properties']['variable_name'] = {
                'type': 'string',
                'description': 'The exact column name from the dataset to visualize on the map. This parameter is REQUIRED. Extract the variable name from the user request.',
            }
            base_params['required'].append('variable_name')
        
        if tool_name == 'create_settlement_map':
            base_params['properties'].update({
                'ward_name': {
                    'type': 'string',
                    'description': 'Optional specific ward name to highlight and focus on'
                },
                'zoom_level': {
                    'type': 'integer',
                    'description': 'Map zoom level (11=city view, 13=ward view, 15=detailed)',
                    'default': 11
                }
            })
        
        if tool_name == 'show_settlement_statistics':
            # No additional parameters needed - just session_id
            pass
        
        if tool_name == 'run_itn_planning':
            base_params['properties'].update({
                'total_nets': {
                    'type': 'integer',
                    'description': 'Total number of bed nets available for distribution (e.g., 50000, 100000)'
                },
                'avg_household_size': {
                    'type': 'number',
                    'description': 'Average household size in the area (default: 5.0)'
                },
                'urban_threshold': {
                    'type': 'number',
                    'description': 'Urban percentage threshold for prioritization (default: 30.0)'
                },
                'method': {
                    'type': 'string',
                    'enum': ['composite', 'pca'],
                    'description': 'Ranking method to use (default: composite)'
                }
            })

        if tool_name == 'list_dataset_columns':
            base_params['properties'].update({
                'page': {
                    'type': 'integer',
                    'description': 'Page number to display (1-indexed)',
                    'default': 1
                },
                'page_size': {
                    'type': 'integer',
                    'description': 'Number of columns per page (default 15)',
                    'default': 15
                }
            })

        return base_params
    
    

    # Legacy _build_system_prompt removed - now using PromptBuilder
    # See archive/legacy_prompt_builder.py for the original 400+ line method
    def _build_system_prompt_refactored(self, session_context: Dict, session_id: str = None) -> str:
        """Refactored system prompt using PromptBuilder; minimal fallback on error."""
        try:
            if hasattr(self, 'prompt_builder') and self.prompt_builder is not None:
                sc = dict(session_context) if isinstance(session_context, dict) else session_context
                if session_id and isinstance(sc, dict):
                    sc.setdefault('session_id', session_id)
                return self.prompt_builder.build(sc, session_id)
        except Exception as e:
            logger.warning(f"PromptBuilder failed, falling back to minimal prompt: {e}")
        return "You are ChatMRPT, an AI assistant for malaria risk analysis. Be accurate, concise, and action-oriented."
