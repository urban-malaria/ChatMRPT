"""Display names for DHIS2 places.

DHIS2 org units carry a two-letter state code and a type suffix ("kw Kwara State",
"kw Moro Local Government Area", "kw Ala0mu Ward"). Data files keep the raw names; text shown to
users uses these cleaned forms.
"""
from __future__ import annotations

import re

_CODE_PREFIX = re.compile(r'^[a-z]{2}\s+(?=[A-Z0-9])')
_LGA_SUFFIX = re.compile(r'\s+(Local Government Area|LGA)$', re.IGNORECASE)
_WARD_SUFFIX = re.compile(r'\s+Ward$', re.IGNORECASE)
_STATE_SUFFIX = re.compile(r'(\s+State)+$', re.IGNORECASE)


def state_name(name: object) -> str:
    """'kw Kwara State' -> 'Kwara'."""
    text = _CODE_PREFIX.sub('', str(name or '').strip())
    return _STATE_SUFFIX.sub('', text).strip()


def lga_name(name: object) -> str:
    """'kw Moro Local Government Area' -> 'Moro'."""
    return _LGA_SUFFIX.sub('', _CODE_PREFIX.sub('', str(name or '').strip())).strip()


def ward_name(name: object) -> str:
    """'kw Ala0mu Ward' -> 'Ala0mu'."""
    return _WARD_SUFFIX.sub('', _CODE_PREFIX.sub('', str(name or '').strip())).strip()
