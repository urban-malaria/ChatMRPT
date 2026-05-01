"""
Response formatter for WhatsApp.

WhatsApp has a 1600-character message limit. Long ChatMRPT responses
(analysis summaries, explanations) are split at sentence boundaries so
each part reads naturally.
"""

import re

_LIMIT = 1590  # leave 10 chars headroom below WhatsApp's 1600 limit


def chunk_text(text: str) -> list[str]:
    """
    Split text into ≤1590-char chunks at sentence boundaries.
    Falls back to word boundaries if no sentence boundary fits.
    Returns a list of strings (usually just one for short responses).
    """
    text = text.strip()
    if len(text) <= _LIMIT:
        return [text]

    chunks = []
    # Split on sentence-ending punctuation followed by whitespace
    sentences = re.split(r'(?<=[.!?])\s+', text)
    current = ''

    for sentence in sentences:
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
            current = (current + ' ' + sentence).strip()

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
