# Arena Mode Documentation

## Overview

Arena Mode allows users to compare different LLM models in a blind tournament format. Users see two responses side-by-side and vote for their preferred answer. The winner advances until a champion is determined.

## When Arena Triggers

Arena mode **automatically triggers** for general knowledge questions in the chat. The system routes questions based on content:

| Message Type | Arena Triggers? | Example |
|--------------|-----------------|---------|
| General knowledge | **Yes** | "What causes malaria?" |
| Educational/explanatory | **Yes** | "Explain PCA analysis" |
| Methodology questions | **Yes** | "How does risk scoring work?" |
| Knowledge + has data | **Yes** | "What is malaria transmission?" (even with uploaded data) |
| Greetings | No | "hi", "hello" |
| Pleasantries | No | "thanks", "ok", "bye" |
| Short messages (<3 words) | No | "What is" |
| Data analysis requests | No | "Run risk analysis on my data" |
| Tool-based requests | No | "Create a vulnerability map" |
| Questions about user's data | No | "What's in my uploaded data?" |

### Eligibility Logic

Arena only activates when:
1. `ARENA_ENABLED=true` in environment
2. `GROQ_API_KEY` is set
3. Routing decision is `can_answer` (no tools needed)
4. Message is substantive (not greeting/pleasantry, 3+ words)

**Important:** Knowledge questions trigger arena even if user has uploaded data. The routing detects patterns like "what is", "how does", "explain", etc. and routes them to arena unless they explicitly reference the user's data ("my data", "uploaded", etc.).

Helpers:
- `app/web/routes/analysis/chat_routing.py` - Routing logic
- `app/web/routes/analysis/arena_helpers.py` - Eligibility checking

## Architecture

### Tournament Structure

```
Round 1: Model A vs Model B (Groq)
Round 2: Winner vs Model C (Groq)
Round 3: Champion vs GPT-4o (Final - OpenAI)
```

The 3 Groq models compete first, then the champion faces GPT-4o in the final round.

### Models

| Model | Provider | Size | Display Name |
|-------|----------|------|--------------|
| qwen/qwen3-32b | Groq | 32B | Qwen 3 32B |
| llama-3.3-70b-versatile | Groq | 70B | Llama 3.3 70B |
| moonshotai/kimi-k2-instruct-0905 | Groq | ~200B | Kimi K2 |
| gpt-4o | OpenAI | - | GPT-4o (Final) |

## Configuration

### Environment Variables

```bash
# Required
GROQ_API_KEY=gsk_xxx              # Get free at console.groq.com
OPENAI_API_KEY=sk-xxx             # For GPT-4o final round

# Optional
ARENA_ENABLED=true                 # Enable/disable arena (default: true)
ARENA_INCLUDE_OPENAI=true          # Include GPT-4o as final challenger
ARENA_MAX_TOKENS=800               # Max tokens per response
```

### Config File

`app/config/arena.py` - Central configuration for Arena mode.

## API Endpoints

### GET /api/arena/status

Check if Arena is available.

**Response:**
```json
{
  "available": true,
  "models": ["qwen/qwen3-32b", "llama-3.3-70b-versatile", "moonshotai/kimi-k2-instruct-0905", "gpt-4o"],
  "total_battles": 42,
  "leaderboard": [...]
}
```

### POST /api/arena/start_battle

Start a new tournament.

**Request:**
```json
{
  "message": "What causes malaria?",
  "session_id": "optional-session-id"
}
```

**Response:**
```json
{
  "battle_id": "uuid",
  "status": "ready",
  "model_a": "qwen/qwen3-32b",
  "model_b": "llama-3.3-70b-versatile",
  "response_a": "...",
  "response_b": "...",
  "current_round": 1,
  "total_models": 4
}
```

### POST /api/arena/vote

Submit vote and get next round or final results.

**Request:**
```json
{
  "battle_id": "uuid",
  "choice": "left" | "right" | "tie"
}
```

**Response (more rounds):**
```json
{
  "continue": true,
  "current_round": 2,
  "model_a": "qwen/qwen3-32b",
  "model_b": "moonshotai/kimi-k2-instruct-0905",
  "response_a": "...",
  "response_b": "..."
}
```

**Response (tournament complete):**
```json
{
  "continue": false,
  "winner": "gpt-4o",
  "final_ranking": ["gpt-4o", "qwen/qwen3-32b", "moonshotai/kimi-k2-instruct-0905", "llama-3.3-70b-versatile"]
}
```

### GET /api/arena/leaderboard

Get model rankings.

**Response:**
```json
{
  "leaderboard": [
    {
      "rank": 1,
      "model": "qwen/qwen3-32b",
      "display_name": "Qwen 3 32B",
      "elo_rating": 1539.0,
      "win_rate": 80.0
    }
  ]
}
```

## Data Collection

Arena collects the following data for analysis:

| Data | Location | Description |
|------|----------|-------------|
| ELO Ratings | `instance/arena_stats.json` | Per-model ELO scores |
| Match History | `instance/arena_stats.json` | Winner/loser + timestamps |
| Preference Stats | `instance/arena_stats.json` | Left/right/tie distribution |
| Model Usage | `instance/arena_stats.json` | How often each model is used |
| Battle Sessions | Redis (or fallback) | Full battle details with responses |

### Stats File Structure

```json
{
  "elo_ratings": {
    "qwen/qwen3-32b": 1539.0,
    "llama-3.3-70b-versatile": 1455.6,
    "moonshotai/kimi-k2-instruct-0905": 1486.0,
    "gpt-4o": 1518.6
  },
  "match_history": [
    {
      "winner": "qwen/qwen3-32b",
      "loser": "llama-3.3-70b-versatile",
      "winner_rating_after": 1539.0,
      "loser_rating_after": 1455.6,
      "timestamp": "2026-01-24T22:40:50.057405"
    }
  ],
  "model_usage": {
    "qwen/qwen3-32b": 10,
    "llama-3.3-70b-versatile": 12
  },
  "preference_stats": {
    "left": 25,
    "right": 18,
    "tie": 3,
    "both_bad": 1
  }
}
```

## File Structure

```
app/
├── config/
│   └── arena.py              # Arena configuration
├── core/
│   ├── arena_manager.py      # Main arena logic (~520 lines)
│   ├── arena_system_prompt.py # System prompts for models
│   └── llm_adapter.py        # LLM backend adapter (Groq/OpenAI)
└── web/routes/
    ├── arena_routes.py       # API endpoints (~275 lines)
    └── analysis/
        └── arena_helpers.py  # Chat integration helpers
```

## Cost

| Provider | Cost | Daily Limit |
|----------|------|-------------|
| Groq | Free | 14,400 requests/day |
| OpenAI | ~$0.005/1K tokens | Based on API key |

With 3 Groq models + 1 OpenAI model per tournament:
- ~3 Groq calls per tournament (free)
- ~1 OpenAI call per tournament (~$0.01)

## Deployment

### Local Testing

```bash
# Set environment variables
export GROQ_API_KEY=gsk_xxx
export ARENA_INCLUDE_OPENAI=true

# Start server
python run.py

# Test
curl http://localhost:5013/api/arena/status
```

### AWS Deployment

Add to each EC2 instance's environment:

```bash
# SSH to instance
ssh -i /tmp/chatmrpt-key2.pem ec2-user@3.21.167.170

# Add to .env
echo "GROQ_API_KEY=gsk_xxx" >> /home/ec2-user/ChatMRPT/.env
echo "ARENA_INCLUDE_OPENAI=true" >> /home/ec2-user/ChatMRPT/.env

# Restart service
sudo systemctl restart chatmrpt
```

## Troubleshooting

### "Arena is disabled or GROQ_API_KEY not set"

- Check `GROQ_API_KEY` is set in `.env`
- Check `ARENA_ENABLED=true` in `.env`

### Model errors

- Check Groq model availability at https://console.groq.com/docs/models
- Some models get deprecated - update `app/config/arena.py` if needed

### Redis connection errors

- Arena works without Redis using fallback in-memory storage
- For production, ensure Redis is configured for multi-worker support
