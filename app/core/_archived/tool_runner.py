"""
ToolRunner: Bridges LLM function calls to the tool registry (and fallbacks).

- Provides OpenAI function schemas from the registry
- Executes tools by name with normalized result format
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Callable

from .tool_registry import get_tool_registry


class ToolRunner:
    def __init__(self, fallbacks: Optional[Dict[str, Callable[..., Any]]] = None) -> None:
        self.registry = get_tool_registry()
        self.fallbacks = fallbacks or {}

    def get_function_schemas(self) -> List[Dict[str, Any]]:
        """Return OpenAI-compatible function schemas from registry AND fallback tools.

        Note: We rely on registry metadata for rich descriptions/examples.
        """
        schemas = []

        # Get schemas from tool registry
        try:
            schemas.extend(self.registry.get_tool_schemas())
        except Exception:
            pass

        # CRITICAL FIX: Add schemas for fallback tools (execute_sql_query, execute_data_query, etc.)
        for tool_name, tool_func in self.fallbacks.items():
            # Build schema from function signature and docstring
            schema = {
                'name': tool_name,
                'description': tool_func.__doc__ or f"Execute {tool_name}",
                'parameters': self._build_parameters_from_function(tool_func)
            }
            schemas.append(schema)

        return schemas

    def _build_parameters_from_function(self, func: Callable) -> Dict[str, Any]:
        """Build OpenAI parameters schema from function signature."""
        import inspect

        sig = inspect.signature(func)
        properties = {}
        required = []

        for param_name, param in sig.parameters.items():
            if param_name == 'session_id':
                continue  # Skip session_id, it's injected automatically

            # Determine type from annotation
            param_type = "string"  # default
            if param.annotation != inspect.Parameter.empty:
                annotation_str = str(param.annotation)
                if 'int' in annotation_str.lower():
                    param_type = "integer"
                elif 'float' in annotation_str.lower():
                    param_type = "number"
                elif 'bool' in annotation_str.lower():
                    param_type = "boolean"

            properties[param_name] = {
                "type": param_type,
                "description": f"The {param_name} parameter"
            }

            # Check if required (no default value)
            if param.default == inspect.Parameter.empty:
                required.append(param_name)

        return {
            "type": "object",
            "properties": properties,
            "required": required
        }

    def execute(self, function_name: str, arguments_json: str) -> Dict[str, Any]:
        """Execute a tool by name with JSON arguments string.

        Returns a normalized dict with keys: response|message, status, tools_used,
        and optional visualizations/download_links.
        """
        try:
            args = json.loads(arguments_json) if arguments_json else {}
        except Exception:
            args = {}

        session_id = args.pop("session_id", None)

        # Normalize function name (handle camelCase, snake_case, lowercase variations)
        normalized_name = self._find_matching_tool_name(function_name)

        # Try registry first
        if session_id and normalized_name:
            tool_obj = self.registry.get_tool(normalized_name)
            if not tool_obj:
                # If the name exists in cached schemas but actual tool not loaded,
                # trigger discovery once to populate executable tool classes.
                try:
                    available_names = set(self.registry.list_tools())
                except Exception:
                    available_names = set()
                if normalized_name in available_names:
                    # Attempt to populate registry with real tool classes
                    try:
                        self.registry.discover_tools()
                        tool_obj = self.registry.get_tool(normalized_name)
                    except Exception:
                        tool_obj = None
            if tool_obj:
                result = self.registry.execute_tool(normalized_name, session_id=session_id, **args)
                return self._normalize_registry_result(normalized_name, result)

        # Fallback (bound functions passed in by the interpreter)
        if normalized_name and normalized_name in self.fallbacks:
            try:
                result = self.fallbacks[normalized_name](session_id=session_id, **args)
                return self._normalize_legacy_result(normalized_name, result)
            except Exception as e:
                return {
                    "response": f"Error executing {normalized_name}: {str(e)}",
                    "status": "error",
                    "tools_used": []
                }

        return {
            "response": f"Unknown function: {function_name}",
            "status": "error",
            "tools_used": []
        }

    def _find_matching_tool_name(self, requested_name: str) -> Optional[str]:
        """Find the actual tool name matching the requested name (case-insensitive, handles variations).

        Handles:
        - Exact match
        - Case-insensitive match
        - camelCase -> snake_case conversion
        - All lowercase with no separators
        """
        import logging
        logger = logging.getLogger(__name__)

        # Try exact match first
        all_tool_names = self.registry.list_tools()
        logger.info(f"🔍 Tool matching: requested='{requested_name}', registry has {len(all_tool_names)} tools")

        if requested_name in all_tool_names:
            logger.info(f"✅ Exact match found: {requested_name}")
            return requested_name

        # Try exact match in fallbacks
        if requested_name in self.fallbacks:
            return requested_name

        # Try case-insensitive match
        requested_lower = requested_name.lower()
        for tool_name in all_tool_names:
            if tool_name.lower() == requested_lower:
                return tool_name

        # Try removing all non-alphanumeric and comparing
        requested_alphanum = ''.join(c for c in requested_name if c.isalnum()).lower()
        logger.info(f"🔍 Trying alphanum match: '{requested_alphanum}'")

        for tool_name in all_tool_names:
            tool_alphanum = ''.join(c for c in tool_name if c.isalnum()).lower()
            if tool_alphanum == requested_alphanum:
                logger.info(f"✅ Alphanum match found: '{tool_name}' matches '{requested_name}'")
                return tool_name

        # Try same with fallbacks
        for fallback_name in self.fallbacks.keys():
            fallback_alphanum = ''.join(c for c in fallback_name if c.isalnum()).lower()
            if fallback_alphanum == requested_alphanum:
                logger.info(f"✅ Fallback match found: {fallback_name}")
                return fallback_name

        logger.warning(f"❌ No match found for '{requested_name}'")
        logger.info(f"📋 Available tools (first 10): {all_tool_names[:10]}")
        return None

    def _normalize_registry_result(self, function_name: str, result: Dict[str, Any]) -> Dict[str, Any]:
        # Registry returns dict with status/message/data/etc.
        if not isinstance(result, dict):
            return {
                "response": str(result),
                "status": "success",
                "tools_used": [function_name]
            }

        msg = result.get("message") or result.get("response") or ""
        visualizations = []
        data = result.get("data") or {}

        # Check for web_path in data dict (legacy format)
        if isinstance(data, dict) and data.get("web_path"):
            visualizations.append({
                "type": data.get("map_type", data.get("chart_type", "visualization")),
                "path": data.get("web_path", ""),
                "url": data.get("web_path", ""),
                "file_path": data.get("file_path", ""),
                "title": result.get("message", "Visualization")
            })
        # CRITICAL FIX: Also check for top-level web_path (ITN tool format)
        elif result.get("web_path"):
            visualizations.append({
                "type": data.get("map_type", data.get("chart_type", "itn_distribution_map")),
                "path": result.get("web_path", ""),
                "url": result.get("web_path", ""),
                "file_path": data.get("file_path", ""),
                "title": result.get("message", "Visualization")
            })

        return {
            "response": msg or str(result),
            "status": result.get("status", "success"),
            "visualizations": visualizations,
            "download_links": result.get("download_links", []),
            "tools_used": [function_name]
        }

    def _normalize_legacy_result(self, function_name: str, result: Any) -> Dict[str, Any]:
        if isinstance(result, dict) and "response" in result:
            return {
                "response": result.get("response", ""),
                "status": result.get("status", "success"),
                "visualizations": result.get("visualizations", []),
                "download_links": result.get("download_links", []),
                "tools_used": result.get("tools_used", [function_name])
            }
        # String or other simple values
        return {
            "response": result if isinstance(result, str) else str(result),
            "status": "success",
            "tools_used": [function_name]
        }
