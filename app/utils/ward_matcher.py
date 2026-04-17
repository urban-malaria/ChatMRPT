"""
Canonical ward name normalizer and fuzzy matcher.
Single source of truth — all TPR, ITN, and visualization code imports from here.
"""

import re
import difflib
from typing import Optional

_TWO_LETTER_PREFIX = re.compile(r'^[a-z]{2}\s+', re.IGNORECASE)
_WARD_KEYWORD      = re.compile(r'\bward\b', re.IGNORECASE)
_SEPARATORS        = re.compile(r'[/\-]+')

# Longer patterns before shorter — avoids partial replacement (viii before vii before vi)
_ROMAN = [
    (r'\bviii\b', '8'), (r'\bvii\b', '7'), (r'\bvi\b', '6'),
    (r'\bix\b',   '9'), (r'\biv\b',  '4'), (r'\bv\b',  '5'),
    (r'\biii\b',  '3'), (r'\bii\b',  '2'), (r'\bi\b',  '1'),
]


def normalize_ward_name(name: str) -> str:
    """
    Normalize a ward name for consistent matching across DHIS2 and shapefile sources.

    Merges behavior from tpr/utils.py (state prefix, Ward keyword, separators)
    and itn_pipeline.py (parentheses, underscores, roman numerals I-IX).
    """
    if not isinstance(name, str) or not name.strip():
        return ''
    s = str(name).strip()
    s = s.split('(')[0]                      # strip parenthetical suffix e.g. "Ward Name (Rural)"
    s = _TWO_LETTER_PREFIX.sub('', s)        # strip 2-letter state prefix e.g. "kw ", "ad "
    s = _WARD_KEYWORD.sub('', s)             # remove "Ward" keyword anywhere
    s = _SEPARATORS.sub(' ', s)              # unify - and / to spaces
    s = s.replace('_', ' ')                  # underscores to spaces
    s = s.lower()
    for pattern, replacement in _ROMAN:
        s = re.sub(pattern, replacement, s)
    return ' '.join(s.split())               # collapse internal whitespace


def fuzzy_match_ward(
    query: str,
    candidates: list,
    cutoff: float = 0.70,
) -> tuple:
    """
    Return (best_match, score) or (None, None).

    Returns (None, None) when:
    - No candidate scores above cutoff
    - Multiple candidates normalize to the same top key (ambiguous — caller must handle)

    Uses difflib SequenceMatcher only. Scores are NOT comparable to fuzzywuzzy.
    """
    q_norm = normalize_ward_name(query)

    # Map normalized key → list of originals (preserves ambiguity instead of silently dropping)
    normed: dict = {}
    for c in candidates:
        key = normalize_ward_name(c)
        normed.setdefault(key, []).append(c)

    matches = difflib.get_close_matches(q_norm, normed.keys(), n=1, cutoff=cutoff)
    if not matches:
        return None, None

    originals = normed[matches[0]]
    if len(originals) > 1:
        # Ambiguous — multiple wards share the same normalized key
        return None, None

    score = difflib.SequenceMatcher(None, q_norm, matches[0]).ratio()
    return originals[0], round(score, 3)
