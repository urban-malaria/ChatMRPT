"""
Arena Mode Configuration

Clean configuration for Arena mode using Groq API.
Replaces the old Ollama-based configuration.
"""

import os

# Arena feature flag
ARENA_ENABLED = os.environ.get('ARENA_ENABLED', 'true').lower() == 'true'

# Groq API configuration
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')

# Arena models configuration
# Models A, B, C are Groq models; Model D (optional) is OpenAI for final comparison
# Updated models list - see https://console.groq.com/docs/models
ARENA_MODELS = {
    'openai/gpt-oss-120b': {
        'label': 'A',
        'provider': 'groq',
        'display_name': 'GPT-OSS 120B',
    },
    'qwen/qwen3.8-27b': {
        'label': 'B',
        'provider': 'groq',
        'display_name': 'Qwen 3.8 27B',
    },
    'openai/gpt-oss-20b': {
        'label': 'C',
        'provider': 'groq',
        'display_name': 'GPT-OSS 20B',
    },
}

# Optionally include OpenAI GPT-4o as the final challenger (Model D)
INCLUDE_OPENAI_FINAL = os.environ.get('ARENA_INCLUDE_OPENAI', 'false').lower() == 'true'
if INCLUDE_OPENAI_FINAL:
    ARENA_MODELS['gpt-4o'] = {
        'label': 'D',
        'provider': 'openai',
        'display_name': 'GPT-4o',
        'is_final': True,
    }

# Response configuration
RESPONSE_TIMEOUT = 30  # seconds
MAX_TOKENS = int(os.environ.get('ARENA_MAX_TOKENS', '1500'))  # Increased for complete responses

# Redis configuration for battle storage
REDIS_TTL_HOURS = 24


def is_arena_available() -> bool:
    """Check if Arena mode is available (enabled and API key present)."""
    return ARENA_ENABLED and bool(GROQ_API_KEY)


def get_model_count() -> int:
    """Get the number of models in the arena."""
    return len(ARENA_MODELS)


def get_groq_models() -> list:
    """Get list of Groq model IDs."""
    return [
        model_id for model_id, config in ARENA_MODELS.items()
        if config.get('provider') == 'groq'
    ]
