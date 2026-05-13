"""
Response formatter for WhatsApp.

WhatsApp has a 1600-character message limit. Long ChatMRPT responses
(analysis summaries, explanations) are split at sentence boundaries so
each part reads naturally.
"""

from __future__ import annotations

import re

_LIMIT = 1590  # leave 10 chars headroom below WhatsApp's 1600 limit


def clean_whatsapp_text(text: str) -> str:
    """Make shared web-agent responses easier to read and copy in WhatsApp."""
    text = (text or "").strip()
    if not text:
        return ""

    replacements = {
        'Run *"malaria risk analysis"*': 'Type: run malaria risk analysis',
        "Run *'malaria risk analysis'*": "Type: run malaria risk analysis",
        'Try: "**run malaria risk analysis**"': 'Type: run malaria risk analysis',
        'Try: "*run malaria risk analysis*"': 'Type: run malaria risk analysis',
        '"run malaria risk analysis"': 'run malaria risk analysis',
        "'run malaria risk analysis'": "run malaria risk analysis",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    # Convert common Markdown styles to WhatsApp-friendly plain text.
    text = re.sub(r"^#{1,6}\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\*\*([^*\n]+)\*\*", r"*\1*", text)
    text = re.sub(r"`([^`\n]+)`", r"\1", text)

    # Avoid awkward WhatsApp messages like: Run *malaria risk analysis*
    text = re.sub(
        r"\bRun\s+\*malaria risk analysis\*",
        "Type: run malaria risk analysis",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bRun\s+malaria risk analysis\b",
        "Type: run malaria risk analysis",
        text,
    )

    # Make common command prompts consistent and copyable.
    command_patterns = (
        "run malaria risk analysis",
        "map malaria burden distribution",
        "show me the highest risk wards",
        "show me the lowest risk wards",
        "show me the vulnerability maps",
        "create composite vulnerability map",
        "I want to plan bed net distribution",
        "Help me distribute ITNs",
        "export results",
    )
    for command in command_patterns:
        text = re.sub(
            rf"(?<!Type: )\b(?:say|type|reply with|just say)\s+\*?{re.escape(command)}\*?",
            f"Type: {command}",
            text,
            flags=re.IGNORECASE,
        )

    # Collapse excessive blank lines without flattening useful WhatsApp spacing.
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def chunk_text(text: str) -> list[str]:
    """
    Split text into ≤1590-char chunks at sentence boundaries.
    Falls back to word boundaries if no sentence boundary fits.
    Returns a list of strings (usually just one for short responses).
    """
    text = clean_whatsapp_text(text)
    if len(text) <= _LIMIT:
        return [text]

    chunks = []
    # Split on paragraph, line, or sentence boundaries while preserving readable spacing.
    sentences = re.split(r'(\n\n+|\n|(?<=[.!?])\s+)', text)
    current = ''

    for sentence in sentences:
        if not sentence:
            continue
        if len(sentence) > _LIMIT:
            # Single sentence exceeds limit — split on word boundaries
            words = sentence.split()
            for word in words:
                if len(current) + len(word) + 1 > _LIMIT:
                    if current:
                        chunks.append(current.strip())
                    current = word
                else:
                    current = (current + ' ' + word).strip()
        elif len(current) + len(sentence) + 1 > _LIMIT:
            if current:
                chunks.append(current.strip())
            current = sentence
        else:
            current = current + sentence

    if current:
        chunks.append(current.strip())

    # Add part indicators if more than one chunk
    if len(chunks) > 1:
        chunks = [f'({i+1}/{len(chunks)}) {c}' for i, c in enumerate(chunks)]

    return chunks


def format_welcome() -> str:
    return (
        "👋 Welcome to *ChatMRPT* — your AI malaria risk analysis assistant.\n\n"
        "You can:\n"
        "• Send a CSV or Excel file to upload your malaria data\n"
        "• Ask questions about malaria analysis, TPR, risk maps, ITN planning\n"
        "• Type *help* at any time for guidance\n\n"
        "How can I help you today?"
    )


def format_upload_ack(filename: str) -> str:
    return (
        f"📂 Received *{filename}*. Processing your data — this may take up to "
        f"60 seconds. I'll send you the results as soon as they're ready."
    )


def format_error() -> str:
    return (
        "⚠️ Something went wrong processing your request. "
        "Please try again or visit the web app at the ChatMRPT URL."
    )
