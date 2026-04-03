"""
RequestInterpreter for ChatMRPT

Handles conversational routing: pre-upload queries go through LLM,
post-upload data queries are routed to the V3 DataExplorationAgent.
"""

import logging
import json
import time
from typing import Dict, Any, Optional

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
        
        # Conversation storage
        self.conversation_history = {}
        self.session_data = {}
        self._memory_summary_tracker: Dict[str, Dict[str, Any]] = {}
        
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

            self.data_repo = DataRepository()
            self.context_service = SessionContextService(self.data_repo)
            self.prompt_builder = PromptBuilder()
        except Exception as e:
            logger.warning(f"Services init failed (non-fatal): {e}")
            self.context_service = None
            self.prompt_builder = None
        
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

            # ONE-BRAIN: No data loaded = conversational. Data loaded = V3 agent handles
            # (post-upload queries go to /api/v1/data-analysis/chat, not here)
            if not session_context.get('data_loaded', False):
                return self._simple_conversational_response(user_message, session_context, session_id)

            # Fallback: if somehow a data query reaches here, route to agent
            logger.info(f"🔄 Routing to DataExplorationAgent (fallback)")
            try:
                from app.data_analysis_v3.core.data_exploration_agent import DataExplorationAgent
                agent = DataExplorationAgent(session_id)
                agent_result = agent.analyze_sync(user_message)
                response_text = agent_result.get('message', 'Analysis complete.')
                self._store_conversation(session_id, user_message, response_text)
                return {
                    'status': 'success',
                    'response': response_text,
                    'visualizations': agent_result.get('visualizations', []),
                    'tools_used': ['analyze_data'],
                    'total_time': time.time() - start_time,
                }
            except Exception as e:
                logger.warning(f"DataExplorationAgent failed: {e}")
                return {
                    'status': 'error',
                    'response': f'Analysis error: {str(e)}',
                    'tools_used': [],
                    'total_time': time.time() - start_time,
                }
            
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

            # ONE-BRAIN: No data = conversational streaming.
            # Data queries go to /api/v1/data-analysis/chat (V3 agent), not here.
            if not session_context.get('data_loaded', False):
                logger.info(f"❌ No data loaded, using conversational streaming")
                if self.llm_manager and hasattr(self.llm_manager, 'generate_with_functions_streaming'):
                    system_prompt = self._build_system_prompt_refactored(session_context, session_id)

                    conv_messages = list(self.conversation_history.get(session_id, [])[-8:])
                    conv_messages.append({"role": "user", "content": user_message})

                    for chunk in self.llm_manager.generate_with_functions_streaming(
                        messages=conv_messages,
                        system_prompt=system_prompt,
                        functions=[],
                        temperature=0.7,
                        session_id=session_id
                    ):
                        content = chunk.get('content', '')
                        if content:
                            yield {
                                'content': content,
                                'status': 'success',
                                'done': False
                            }

                    yield {'content': '', 'status': 'success', 'done': True}
                else:
                    response = self._simple_conversational_response(user_message, session_context, session_id)
                    yield {
                        'content': response.get('response', ''),
                        'status': 'success',
                        'done': True
                    }
                return

            # Fallback: data query reached streaming endpoint — shouldn't happen
            # but route to agent just in case
            logger.warning(f"Data query reached streaming endpoint — routing to agent")
            try:
                from app.data_analysis_v3.core.data_exploration_agent import DataExplorationAgent
                agent = DataExplorationAgent(session_id)
                agent_result = agent.analyze_sync(user_message)
                yield {
                    'content': agent_result.get('message', 'Analysis complete.'),
                    'status': 'success',
                    'visualizations': agent_result.get('visualizations', []),
                    'tools_used': ['analyze_data'],
                    'done': True,
                }
            except Exception as agent_err:
                logger.error(f"Agent fallback failed: {agent_err}")
                yield {
                    'content': f'Error: {agent_err}',
                    'status': 'error',
                    'done': True,
                }
            
        except Exception as e:
            logger.error(f"Error in streaming: {e}")
            yield {
                'content': f'I encountered an issue: {str(e)}',
                'status': 'error',
                'done': True
            }
    
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
