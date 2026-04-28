"""
Known DHIS2 export mojibake patterns.

DHIS2 pivot table exports occasionally corrupt template variables during
XLS/CSV generation, producing `0me` where `name` should appear (pattern: na → 0).
This is a known issue seen in Kwara state's 2020-2025 malaria data export and
possibly others.

Patterns are applied in order. Each pattern is (regex, replacement) where the
regex is matched against the full column name (anchored) or a substring
(word-bounded) depending on the pattern style.

Add new patterns here as they are discovered in production. Do NOT attempt
a generic `0 → na` replacement — that would corrupt legitimate column names.
"""

import re
from typing import List, Tuple

# Each entry: (compiled_regex, replacement_template)
# The regex is applied with re.sub so backreferences are supported.
MOJIBAKE_PATTERNS: List[Tuple[re.Pattern, str]] = [
    # Full-column-name patterns (anchored with ^...$)
    # These catch DHIS2 template variable corruption where the whole column name
    # is exactly the broken identifier.
    (re.compile(r'^period0me(\.\d+)?$'), r'periodname\1'),
    (re.compile(r'^organisationunit0me(\.\d+)?$'), r'organisationunitname\1'),
    (re.compile(r'^categoryoptioncomboid0me(\.\d+)?$'), r'categoryoptioncomboname\1'),
    (re.compile(r'^attributeoptioncomboid0me(\.\d+)?$'), r'attributeoptioncomboname\1'),

    # Substring patterns for malaria-specific terminology corruption
    # These are applied anywhere in the column name (no anchors).
    (re.compile(r'Artesu0te'), 'Artesunate'),
    (re.compile(r'Arte0te'), 'Artesunate'),
]


def apply_mojibake_fixes(column_name: str) -> str:
    """
    Apply all known mojibake patterns to a column name.
    Returns the cleaned name (may be unchanged if no pattern matches).
    """
    result = column_name
    for pattern, replacement in MOJIBAKE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result
