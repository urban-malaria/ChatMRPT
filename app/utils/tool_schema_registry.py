"""
Tool Schema Registry

Centralized JSON-style schemas for tool arguments used by the universal
argument interpreter. The interpreter uses these to constrain outputs and
normalize free-form language into canonical values.
"""

from __future__ import annotations

from typing import Dict, Any


def get_tool_schema(tool_id: str) -> Dict[str, Any]:
    """Return a JSON-like schema describing arguments for a tool.

    Schemas are intentionally compact. They include allowed enums and
    basic numeric constraints where practical. The interpreter is
    responsible for validating against this structure.
    """
    base: Dict[str, Any] = {
        "type": "object",
        "properties": {},
        "required": [],
        "examples": [],
    }

    # ITN Planning
    if tool_id in ("run_itn_planning", "itn"):
        return {
            "type": "object",
            "properties": {
                "total_nets": {"type": "integer", "minimum": 0},
                "avg_household_size": {"type": "number", "minimum": 0},
                "urban_threshold": {"type": "number", "minimum": 0, "maximum": 100},
                "method": {"type": "string", "enum": ["composite", "pca", "both"]},
            },
            "required": [],
            "examples": [
                {"user": "plan ITN with 200k nets", "args": {"total_nets": 200000}},
                {"user": "use PCA method for ITN", "args": {"method": "pca"}},
            ],
        }

    # Vulnerability map
    if tool_id in ("create_vulnerability_map", "map_vulnerability"):
        return {
            "type": "object",
            "properties": {
                "method": {"type": "string", "enum": ["composite", "pca", "both"]},
            },
            "required": [],
            "examples": [
                {"user": "vulnerability map with composite", "args": {"method": "composite"}},
                {"user": "compare both methods", "args": {"method": "both"}},
            ],
        }

    # Variable distribution map
    if tool_id in ("create_variable_distribution", "map_variable"):
        return {
            "type": "object",
            "properties": {
                "map_variable": {"type": "string"},
            },
            "required": ["map_variable"],
            "examples": [
                {"user": "map rainfall", "args": {"map_variable": "rainfall"}},
            ],
        }

    # Risk analysis
    if tool_id in ("run_malaria_risk_analysis", "risk"):
        return {
            "type": "object",
            "properties": {
                "variables": {"type": "array", "items": {"type": "string"}},
            },
            "required": [],
            "examples": [
                {"user": "run risk", "args": {}},
                {"user": "risk with rainfall and elevation", "args": {"variables": ["rainfall", "elevation"]}},
            ],
        }

    # Analyze data (agent hinting only)
    if tool_id in ("analyze_data", "analyze_data_with_python", "data_analysis"):
        return {
            "type": "object",
            "properties": {
                "N": {"type": "integer", "minimum": 1, "maximum": 200},
                "variable": {"type": "string"},
                "chart": {"type": "string"},
            },
            "required": [],
            "examples": [
                {"user": "top 10 wards by rainfall", "args": {"N": 10, "variable": "rainfall"}},
                {"user": "show histogram of TPR", "args": {"chart": "histogram", "variable": "TPR"}},
            ],
        }

    # TPR selections
    if tool_id == "tpr_facility_selection":
        return {
            "type": "object",
            "properties": {
                "facility_level": {"type": "string", "enum": ["primary", "secondary", "tertiary", "all"]},
            },
            "required": ["facility_level"],
            "examples": [
                {"user": "go with primary", "args": {"facility_level": "primary"}},
                {"user": "second one", "args": {"facility_level": "secondary"}},
            ],
        }

    if tool_id == "tpr_age_selection":
        return {
            "type": "object",
            "properties": {
                "age_group": {"type": "string", "enum": ["u5", "o5", "pw", "all_ages"]},
            },
            "required": ["age_group"],
            "examples": [
                {"user": "children under five", "args": {"age_group": "u5"}},
                {"user": "pregnant women", "args": {"age_group": "pw"}},
            ],
        }

    return base
