"""Reviewed explanations of ChatMRPT and its methods.

Entries live in ``app/knowledge/entries/*.md``: YAML front matter followed by a
Markdown body. Every chat path (general chat, data agent, Arena, WhatsApp)
builds its explanations from this library so answers stay consistent.

Entry statuses:
    approved  reviewed and signed off
    draft     written from the code, awaiting review; used in prompts
    hold      contains claims that need a source; never used in prompts
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from yaml import safe_load

logger = logging.getLogger(__name__)

ENTRIES_DIR = Path(__file__).parent / "entries"
VALID_STATUSES = {"approved", "draft", "hold"}
PROMPT_STATUSES = {"approved", "draft"}
REQUIRED_FIELDS = ("id", "title", "summary", "status")

NOT_COVERED_RULE = (
    "Use the ChatMRPT reference below when answering questions about ChatMRPT, its "
    "workflow, data requirements or methods. Do not describe features, methods or "
    "outputs that are not in this reference. If a question about ChatMRPT is not "
    "covered here, say you are not certain and suggest asking the ChatMRPT team, "
    "rather than guessing."
)

_FRONT_MATTER = re.compile(r"\A---\s*\n(.*?)\n---\s*\n?(.*)\Z", re.DOTALL)


@dataclass(frozen=True)
class Entry:
    """One topic in the knowledge library."""

    id: str
    title: str
    summary: str
    body: str
    status: str
    aliases: Tuple[str, ...] = ()
    sources: Tuple[str, ...] = ()
    reviewed_by: str = ""
    reviewed_on: str = ""
    open_questions: Tuple[str, ...] = field(default=(), compare=False)

    @property
    def in_prompts(self) -> bool:
        return self.status in PROMPT_STATUSES

    def full_text(self) -> str:
        return f"### {self.title}\n{self.summary}\n\n{self.body}".strip()


def parse_entry(text: str, source: str = "<string>") -> Entry:
    """Parse one entry file. Raises ValueError if it is malformed."""
    match = _FRONT_MATTER.match(text)
    if not match:
        raise ValueError(f"{source}: missing YAML front matter")
    meta = safe_load(match.group(1)) or {}
    missing = [name for name in REQUIRED_FIELDS if not meta.get(name)]
    if missing:
        raise ValueError(f"{source}: missing required fields {missing}")
    status = str(meta["status"]).strip().lower()
    if status not in VALID_STATUSES:
        raise ValueError(f"{source}: invalid status '{status}'")
    return Entry(
        id=str(meta["id"]).strip(),
        title=str(meta["title"]).strip(),
        summary=" ".join(str(meta["summary"]).split()),
        body=match.group(2).strip(),
        status=status,
        aliases=tuple(str(a).strip().lower() for a in meta.get("aliases") or () if str(a).strip()),
        sources=tuple(str(s).strip() for s in meta.get("sources") or () if str(s).strip()),
        reviewed_by=str(meta.get("reviewed_by") or ""),
        reviewed_on=str(meta.get("reviewed_on") or ""),
        open_questions=tuple(str(q).strip() for q in meta.get("open_questions") or ()),
    )


@lru_cache(maxsize=4)
def load_entries(directory: Optional[str] = None) -> Dict[str, Entry]:
    """Load and validate all entries, keyed by id. Cached per directory."""
    root = Path(directory) if directory else ENTRIES_DIR
    entries: Dict[str, Entry] = {}
    for path in sorted(root.glob("*.md")):
        entry = parse_entry(path.read_text(encoding="utf-8"), source=path.name)
        if entry.id in entries:
            raise ValueError(f"{path.name}: duplicate entry id '{entry.id}'")
        entries[entry.id] = entry
    return entries


def get(entry_id: str) -> Optional[Entry]:
    """Return one entry by id, or None."""
    return load_entries().get(entry_id)


def _alias_pattern(alias: str) -> re.Pattern:
    return re.compile(r"(?<![a-z0-9])" + re.escape(alias) + r"(?![a-z0-9])")


def find(message: str, limit: int = 3) -> List[Entry]:
    """Return prompt-eligible entries whose aliases appear in the message.

    Entries are ranked by the length of their longest matching alias, so
    specific phrases ("composite score") outrank generic ones ("score").
    """
    text = " ".join((message or "").lower().split())
    if not text:
        return []
    scored = []
    for order, entry in enumerate(load_entries().values()):
        if not entry.in_prompts:
            continue
        hits = [len(a) for a in entry.aliases if _alias_pattern(a).search(text)]
        if hits:
            scored.append((-max(hits), -len(hits), order, entry))
    scored.sort(key=lambda item: item[:3])
    return [item[3] for item in scored[:limit]]


def core_brief() -> str:
    """Short reference included in every prompt: one line per topic."""
    lines = [NOT_COVERED_RULE, "", "## ChatMRPT reference"]
    for entry in load_entries().values():
        if entry.in_prompts:
            lines.append(f"- **{entry.title}**: {entry.summary}")
    return "\n".join(lines)


def context_for(message: str, limit: int = 3) -> str:
    """Core brief plus the full text of entries relevant to the message."""
    parts = [core_brief()]
    matched = find(message, limit=limit)
    if matched:
        parts.append("## Details relevant to this question")
        parts.extend(entry.full_text() for entry in matched)
    return "\n\n".join(parts)
