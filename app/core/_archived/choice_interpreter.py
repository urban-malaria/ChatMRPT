"""
Universal Choice/Argument Interpreter

LLM-first resolver that maps free-form user input to schema-valid tool
arguments with confidence and a short reason. Provides small, deterministic
fallbacks when the LLM output cannot be parsed or validated.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

from .tool_schema_registry import get_tool_schema


class ChoiceInterpreter:
    def __init__(self, llm_manager) -> None:
        self.llm = llm_manager

    def resolve(
        self,
        tool_id: str,
        user_text: str,
        memory_summary: Optional[str] = None,
        columns_context: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        schema = get_tool_schema(tool_id)
        prompt = self._build_prompt(tool_id, user_text, schema, memory_summary, columns_context)

        try:
            # Use a constrained call to the LLM
            response = self.llm.generate_response(
                prompt=prompt,
                context={},
                system_message=self._system_message(),
                temperature=0.2,
                max_tokens=300,
                session_id=session_id,
            )
            parsed = self._parse_json(response)
            if parsed and isinstance(parsed.get("args"), dict):
                # Basic numeric coercions where obvious
                parsed["args"] = self._coerce_numbers(parsed["args"])
                parsed.setdefault("confidence", 0.7)
                parsed.setdefault("matched_by", "llm")
                return parsed
        except Exception:
            pass

        # Fallback: deterministic extraction for common patterns
        args, matched = self._fallback_extract(tool_id, user_text)
        if args:
            return {
                "tool": tool_id,
                "args": args,
                "confidence": 0.55,
                "matched_by": matched,
                "reason": "fallback extraction",
            }

        # Final resort: ask for clarification upstream
        return {
            "tool": tool_id,
            "args": {},
            "confidence": 0.0,
            "matched_by": "none",
            "reason": "unable to resolve arguments",
        }

    # ---------------- internal helpers ----------------
    def _system_message(self) -> str:
        return (
            "You extract arguments for tools based on a provided JSON schema. "
            "Output strictly as JSON with keys: tool, args, confidence, reason. "
            "Never include extra text or code fences. Confidence must be between 0 and 1."
        )

    def _build_prompt(
        self,
        tool_id: str,
        user_text: str,
        schema: Dict[str, Any],
        memory_summary: Optional[str],
        columns_context: Optional[Dict[str, Any]],
    ) -> str:
        return json.dumps(
            {
                "task": "resolve_arguments",
                "tool": tool_id,
                "user_text": user_text,
                "memory_summary": memory_summary or "",
                "columns_context": columns_context or {},
                "arg_schema": schema,
                "output_schema": {
                    "tool": "string (tool id)",
                    "args": "object (must validate against arg_schema)",
                    "confidence": "number 0..1",
                    "reason": "string (very short)",
                },
                "instructions": [
                    "Fill only fields defined in arg_schema.",
                    "Return EXACT JSON with tool, args, confidence, reason.",
                    "Use compact, schema-valid values (enums, numbers).",
                ],
            },
            ensure_ascii=False,
        )

    def _parse_json(self, text: str) -> Optional[Dict[str, Any]]:
        if not text:
            return None
        t = text.strip()
        if t.startswith("```"):
            t = t.strip("`")
        try:
            # find JSON object bounds
            s = t.find("{")
            e = t.rfind("}")
            if s != -1 and e != -1 and e > s:
                return json.loads(t[s : e + 1])
            return json.loads(t)
        except Exception:
            return None

    def _coerce_numbers(self, args: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = dict(args)
        for k, v in list(out.items()):
            if isinstance(v, str):
                # Normalize shorthand like 200k, 1.2m, etc.
                m = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([kKmM]?)\s*", v)
                if m:
                    num = float(m.group(1))
                    suffix = m.group(2).lower()
                    if suffix == "k":
                        num *= 1000
                    elif suffix == "m":
                        num *= 1000000
                    # Prefer int when close
                    out[k] = int(num) if abs(num - int(num)) < 1e-6 else num
        return out

    def _fallback_extract(self, tool_id: str, text: str) -> (Dict[str, Any], str):
        t = text.lower()
        # ITN: total nets & method
        if tool_id in ("run_itn_planning", "itn"):
            args: Dict[str, Any] = {}
            # numbers (e.g., 200k nets)
            m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([kKmM]?)\s*(?:net|nets)?", t)
            if m:
                num = float(m.group(1))
                suf = m.group(2).lower()
                if suf == "k":
                    num *= 1000
                elif suf == "m":
                    num *= 1000000
                args["total_nets"] = int(num) if abs(num - int(num)) < 1e-6 else num
            if "pca" in t:
                args["method"] = "pca"
            elif "composite" in t:
                args["method"] = "composite"
            elif "both" in t or "compare" in t:
                args["method"] = "both"
            if args:
                return args, "regex"

        if tool_id in ("create_vulnerability_map", "map_vulnerability"):
            if "pca" in t:
                return {"method": "pca"}, "regex"
            if "composite" in t:
                return {"method": "composite"}, "regex"
            if "both" in t or "compare" in t:
                return {"method": "both"}, "regex"

        if tool_id in ("create_variable_distribution", "map_variable"):
            # e.g., "map rainfall"
            m = re.search(r"map\s+([a-z0-9_\-\s]+)", t)
            if m:
                return {"map_variable": m.group(1).strip()}, "regex"

        # risk variables (simple split on 'and' / commas)
        if tool_id in ("run_malaria_risk_analysis", "risk"):
            m = re.search(r"risk.*?([a-z,\s]+)", t)
            if m:
                raw = m.group(1)
                parts = [p.strip() for p in re.split(r",|and", raw) if p.strip()]
                if parts:
                    return {"variables": parts}, "regex"

        return {}, "none"

