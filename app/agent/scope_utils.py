"""Utilities for managing analysis scope context."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


def normalize_list(values: Any) -> List[str]:
    if values is None:
        return []
    if isinstance(values, str):
        return [values]
    return [str(v) for v in values if v is not None]


def merge_scope(current: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    """Merge incoming scope fields into the current scope, deduplicating lists."""
    scope = dict(current or {})
    for key, value in incoming.items():
        if value is None:
            continue
        if key in {"lgas", "wards", "facilities"}:
            existing = set(scope.get(key, []))
            existing.update(normalize_list(value))
            scope[key] = sorted(existing)
        else:
            scope[key] = value
    return scope


def scope_to_human(scope: Dict[str, Any]) -> str:
    parts: List[str] = []
    if scope.get("lgas"):
        parts.append(f"LGAs: {', '.join(scope['lgas'])}")
    if scope.get("wards"):
        parts.append(f"Wards: {', '.join(scope['wards'])}")
    if scope.get("facilities"):
        parts.append(f"Facilities: {', '.join(scope['facilities'])}")
    if scope.get("period_range"):
        pr = scope['period_range']
        if isinstance(pr, (list, tuple)) and len(pr) == 2:
            parts.append(f"Period: {pr[0]} to {pr[1]}")
        else:
            parts.append(f"Period: {pr}")
    if scope.get("focus"):
        parts.append(f"Focus: {scope['focus']}")
    return " | ".join(parts)
