"""
LLMOrchestrator: Single place to run LLM with function-calling (sync/stream).

- Uses llm_manager for OpenAI calls
- Delegates function execution to ToolRunner
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Optional


class LLMOrchestrator:
    def run_with_tools(
        self,
        llm_manager,
        system_prompt: str,
        user_message: str,
        function_schemas: list[dict],
        session_id: Optional[str],
        tool_runner,
        conversation_history: Optional[list] = None,
    ) -> Dict[str, Any]:
        """Non-streaming function-calling with tool execution."""
        # Build messages with conversation history so the LLM can reference
        # previous turns (what it said, what visualizations it showed, etc.)
        messages = []
        if conversation_history:
            messages.extend(conversation_history[-8:])  # Last 8 turns for context
        messages.append({"role": "user", "content": user_message})

        response = llm_manager.generate_with_functions(
            messages=messages,
            system_prompt=system_prompt,
            functions=function_schemas,
            temperature=0.7,
            session_id=session_id,
        )

        # If model called a function, execute it
        func_call = response.get("function_call") or response.get("tool_call")
        if func_call and func_call.get("name"):
            # CRITICAL FIX: Inject session_id into arguments before calling tool_runner
            args_json = func_call.get("arguments", "{}")
            try:
                args = json.loads(args_json) if args_json else {}
            except:
                args = {}
            args["session_id"] = session_id
            args_json_with_session = json.dumps(args)

            executed = tool_runner.execute(func_call["name"], args_json_with_session)
            return executed

        # Otherwise return conversational content
        return {
            "response": response.get("content", "No response"),
            "status": "success",
            "tools_used": [],
        }

    def stream_with_tools(
        self,
        llm_manager,
        system_prompt: str,
        user_message: str,
        function_schemas: list[dict],
        session_id: Optional[str],
        tool_runner,
        interpretation_cb=None,
        conversation_history: Optional[list] = None,
    ) -> Iterable[Dict[str, Any]]:
        """Streaming function-calling. Executes tool when function_call chunk arrives."""
        messages = []
        if conversation_history:
            messages.extend(conversation_history[-8:])
        messages.append({"role": "user", "content": user_message})

        stream = llm_manager.generate_with_functions_streaming(
            messages=messages,
            system_prompt=system_prompt,
            functions=function_schemas,
            temperature=0.7,
            session_id=session_id,
        )

        accumulated = []
        for chunk in stream:
            # Tool/function call path
            fc = chunk.get("function_call") or chunk.get("tool_call")
            if fc and fc.get("name"):
                # CRITICAL FIX: Inject session_id into arguments before calling tool_runner
                # This ensures tool_runner.execute() can find and execute the tool
                args_json = fc.get("arguments", "{}")
                try:
                    args = json.loads(args_json) if args_json else {}
                except:
                    args = {}
                args["session_id"] = session_id
                args_json_with_session = json.dumps(args)

                executed = tool_runner.execute(fc["name"], args_json_with_session)
                # Emit tool response
                yield {
                    "content": executed.get("response", ""),
                    "status": executed.get("status", "success"),
                    "visualizations": executed.get("visualizations", []),
                    "download_links": executed.get("download_links", []),
                    "tools_used": executed.get("tools_used", [fc.get("name")]),
                    "done": False,
                }
                # Optional interpretation follow-up
                if interpretation_cb:
                    try:
                        # Provide rich context to the callback when available
                        interp = interpretation_cb(
                            executed.get("response", ""),
                            user_message,
                            # The caller can close over session_context; keep signature flexible
                        )
                        if interp:
                            yield {
                                "content": f"\n\n**Analysis:**\n{interp}",
                                "status": executed.get("status", "success"),
                                "tools_used": executed.get("tools_used", []),
                                "done": True,
                            }
                        else:
                            yield {"content": "", "status": "success", "done": True}
                    except Exception:
                        yield {"content": "", "status": "success", "done": True}
                else:
                    yield {"content": "", "status": "success", "done": True}
                return

            # Conversational tokens
            content = chunk.get("content", "")
            if content:
                accumulated.append(content)
                yield {"content": content, "status": "success", "done": False}

            if chunk.get("done"):
                yield {"content": "", "status": "success", "done": True}
                return
