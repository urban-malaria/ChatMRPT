# WhatsApp Integration Plan
**Date:** May 2026  
**Target:** Working prototype by 30 May 2026 for Kwara June campaign  
**Branch:** whatsapp-integration

---

## Goal

Make the full ChatMRPT experience available over WhatsApp. A Kwara programme officer should be able to:
1. Send a CSV/Excel file via WhatsApp → get analysis results back
2. Ask follow-up questions in text → get answers from the LangGraph agent
3. Receive maps as PNG images in the same chat

WhatsApp is a second frontend. The backend (LangGraph, analysis pipeline, upload logic) is unchanged.

---

## Architecture

```
User sends WhatsApp message (text or file)
    ↓
Twilio sandbox (testing) / WhatsApp Cloud API (production)
    ↓
POST /api/whatsapp/webhook  ← new Flask endpoint
    ↓
WhatsApp session manager   ← maps phone number → session_id (stored in Redis)
    ↓
Message router:
  ├── File (CSV/Excel) → existing upload pipeline (handle_csv_only_path / handle_full_dataset_path)
  ├── Text → existing LangGraph agent (send_message flow)
  └── "map" / "show map" → visualization pipeline → PNG image
    ↓
Response formatter:
  ├── Text response > 1600 chars → chunked into multiple messages
  ├── Map/chart → Plotly render as PNG → send via Twilio media
  └── Tables → formatted text or Excel attachment
    ↓
Twilio API sends reply to user
```

---

## Components to Build

### 1. Webhook endpoint — `app/api/whatsapp_routes.py`
- `POST /api/whatsapp/webhook` — receives Twilio POSTs
- Twilio signature validation (TWILIO_AUTH_TOKEN)
- Dispatches to message router
- Returns TwiML response

### 2. Session manager — `app/whatsapp/session.py`
- Maps `whatsapp:+2348XXXXXXXXX` → `session_id` in Redis
- Creates a new ChatMRPT session on first contact
- Stores conversation history per phone number (Redis, TTL 24h)
- Must use Redis (not in-memory — 2 EC2 instances behind ALB)

### 3. Message router — `app/whatsapp/router.py`
- Detects message type: text, document (CSV/Excel), image
- For documents: downloads from Twilio media URL, saves to instance/uploads/, calls upload pipeline
- For text: calls existing LLM/agent pipeline with session context
- For "send map" intent: triggers visualization, converts to PNG

### 4. Response formatter — `app/whatsapp/formatter.py`
- Chunks text into ≤1600 char messages at sentence boundaries
- Converts Plotly HTML maps to PNG (using kaleido or playwright)
- Sends multi-part messages via Twilio API (not TwiML — Twilio REST API)

### 5. Media handler — `app/whatsapp/media.py`
- Downloads documents from Twilio media URLs (authenticated)
- Saves to correct session upload folder
- Returns file path for use by upload pipeline

---

## New Files

```
app/whatsapp/
    __init__.py
    session.py       — Redis-backed session mapping
    router.py        — message type detection + routing
    formatter.py     — text chunking + map → PNG
    media.py         — Twilio media download
app/api/whatsapp_routes.py   — Flask blueprint + webhook endpoint
```

---

## Changes to Existing Files

| File | Change |
|------|--------|
| `app/api/__init__.py` | Import + register `whatsapp_bp` |
| `.env.example` | Add TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM |
| `requirements.txt` | Add `twilio`, `kaleido` |

---

## Environment Variables Needed

```
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=your_auth_token_here
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886   # sandbox number (change for production)
```

---

## Build Order (phases)

### Phase 1 — Text Q&A only (Week 1)
- Webhook endpoint
- Session manager (Redis)
- Text routing → LLM
- Basic response chunking
- **Testable via Twilio sandbox**

### Phase 2 — File uploads (Week 2)
- Media handler (download from Twilio)
- Route CSV/Excel to existing upload pipeline
- Reply with analysis summary

### Phase 3 — Maps as images (Week 2-3)
- Plotly → PNG rendering
- Send PNG via Twilio media API
- Trigger on "show map" / "vulnerability map" intent

---

## Key Constraints

1. **Redis required** — in-memory sessions won't work across 2 EC2 instances
2. **Twilio sandbox** — limited to verified numbers for testing; production needs Google Voice or dedicated number registered with WhatsApp Cloud API
3. **Plotly → PNG** — requires `kaleido` (headless rendering); needs to be installed on EC2
4. **Webhook must return 200 within 15s** — long-running analysis must be async (reply "Processing..." immediately, send result when done)
5. **1600 char limit** — all text responses must be chunked

---

## Open Questions

1. Should we reply "Processing your data, please wait..." immediately for uploads, then send results async? (Recommended — avoids Twilio timeout)
2. What language should the bot greet users in — English only, or detect and respond in Hausa/Yoruba/Igbo?
3. For the Kwara pilot — which specific workflows do they need most? (TPR calculation, vulnerability map, ITN allocation?)
