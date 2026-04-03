# WhatsApp Integration Research for ChatMRPT
**Date:** March 27, 2026
**Purpose:** Research findings to inform WhatsApp integration diagram and grant proposal

---

## 1. Is This Feasible?

**Yes.** This is a well-established pattern with strong precedent in African health contexts.

### Precedents in African Health

| Project | Organization | Scale | What it does |
|---------|-------------|-------|-------------|
| MomConnect | South Africa Dept of Health + Reach Digital Health | 4.7M+ users, 60% of pregnant women in SA | Maternal health messaging via WhatsApp, 11 languages, SMS fallback for low bandwidth |
| HealthConnect | South Africa | 15.6M reach | National health messaging via WhatsApp |
| Helpmum | Nigeria | - | Vaccination chatbot for nursing mothers via WhatsApp |
| GREAT4Diabetes | South Africa | - | Diabetes education chatbot on WhatsApp during COVID |
| Shesha | South Africa | - | TB treatment/preventive therapy chatbot on WhatsApp |
| WHO Health Alert | Global (200+ countries) | Millions | COVID-19 information chatbot on WhatsApp |

**Key insight from MomConnect:** In low-bandwidth areas, users can fall back to SMS. WhatsApp engaged users 10x more effectively than SMS. They shifted from SMS to WhatsApp in 2017 and saw better outcomes at lower cost.

Sources:
- https://www.health.gov.za/momconnect/
- https://www.reachdigitalhealth.org/resources/momconnect-chatbot
- https://www.exemplars.health/emerging-topics/epidemic-preparedness-and-response/digital-health-tools/healthconnect-in-south-africa

---

## 2. Technical Architecture

The architecture is straightforward and maps directly to our existing stack:

```
User sends WhatsApp message
    ↓
WhatsApp Cloud API (Meta)
    ↓
Webhook (HTTP POST) → FastAPI endpoint on our server
    ↓
Server extracts message text/media
    ↓
Routes to ChatMRPT's existing pipeline:
  - LangGraph agent orchestration
  - Gemini LLM processing
  - Bayesian model inference
  - Analysis tools
    ↓
Response formatted for WhatsApp (text + images)
    ↓
Server sends response via WhatsApp Cloud API
    ↓
User receives response on WhatsApp
```

### Existing production examples of this exact pattern:

- **Gemini + WhatsApp:** Multiple implementations documented on GitHub and blogs
  - Source: https://cubed.run/blog/building-an-intelligent-whatsapp-chatbot-with-gemini-llm
  - Source: https://github.com/YonkoSam/whatsapp-python-chatbot

- **LangGraph + WhatsApp:** Infobip has a full tutorial on this exact integration
  - Architecture: FastAPI webhook → LangGraph agent → LLM → WhatsApp response
  - Uses MemorySaver for conversation state per phone number
  - Source: https://www.infobip.com/docs/tutorials/integrate-genai-into-whatsapp-chatbot-with-langgraph-ai-agent

- **GPT-4o + WhatsApp + FastAPI:** Towards Data Science article
  - Source: https://towardsdatascience.com/creating-a-whatsapp-ai-agent-with-gpt-4o-f0bc197d2ac0/

---

## 3. WhatsApp Cloud API — Technical Details

### What it is
Meta's free-to-use API for sending/receiving WhatsApp messages programmatically. No need for a third-party provider (though Twilio, Infobip, etc. are alternatives).

### How webhooks work
- You register a webhook URL (e.g., `https://your-server.com/webhook`)
- When a user sends a message, WhatsApp Cloud API sends an HTTP POST to your webhook
- Your server processes the message and sends a response back via the API
- Thread pooling recommended to avoid blocking the webhook (WhatsApp resends if no 200 response)

### Message types supported
- **Text:** Up to 1,600 characters per message (free-form), 4,096 for template headers
- **Images:** JPEG, PNG — max 5MB
- **Audio:** AAC, AMR, MP3, M4A, OGG (Opus) — max 16MB
- **Video:** MP4, 3GPP — max 16MB
- **Documents:** PDF, DOC, XLSX, etc. — max 100MB
- **Interactive:** Buttons (up to 3), lists (up to 10 items)
- **Location:** Latitude/longitude sharing

### File size limits

| Type | Max Size | Formats |
|------|----------|---------|
| Images | 5 MB | JPEG, PNG |
| Audio | 16 MB | AAC, AMR, MP3, M4A, OGG |
| Video | 16 MB | MP4, 3GPP |
| Documents | 100 MB | PDF, DOC, XLSX, etc. |
| Upload limit (to media node) | 64 MB | Any |

### Rate limits
- New accounts start with 250 business-initiated conversations per 24 hours
- Scales up to 1,000 → 10,000 → 100,000 → unlimited based on quality rating
- Service conversations (user-initiated): no hard limit

Sources:
- https://help.sleekflow.io/en_US/supported-message-types-on-whatsapp-business-api-cloud-a
- https://api.support.vonage.com/hc/en-us/articles/10900821425308
- https://docs.aws.amazon.com/social-messaging/latest/userguide/supported-media-types.html

---

## 4. Pricing

### New model (effective July 1, 2025 — per-message, not per-conversation)

| Category | Description | Nigeria Cost (approx) |
|----------|-------------|----------------------|
| Marketing | Promotional messages | ~$0.052 per message |
| Utility | Order updates, confirmations | ~$0.008 per message |
| Authentication | OTP, verification | ~$0.004 per message |
| Service | User-initiated (within 24hr window) | FREE |

### Key cost facts:
- **Service conversations are FREE** — when users message you first and you respond within 24 hours, it costs nothing
- **Utility messages within service window are FREE** — as of July 2025
- **For ChatMRPT:** Most interactions will be user-initiated (they send a question, we respond) → falls under FREE service conversations
- **Estimated cost:** Minimal. If 1,000 users across 36 states each initiate 5 conversations/month, and all are service conversations → $0/month for messaging
- **Template messages** (proactive alerts) would cost ~$0.008-0.052 per message depending on category

### Nigeria-specific note:
As of January 2026, Meta introduced Naira billing for Nigerian businesses, meaning costs are charged in local currency.

Sources:
- https://www.flowcall.co/blog/whatsapp-business-api-pricing-2026
- https://authkey.io/blogs/whatsapp-pricing-update-2026/
- https://www.naijatechguide.com/whatsapp-business-api-price-guide.html

---

## 5. Setup Requirements

### What you need:
1. **Meta Business account** (free to create)
2. **Meta Business verification** (submit business documents — recommended for higher rate limits)
3. **Dedicated phone number** — must NOT be linked to any existing WhatsApp account
4. **WhatsApp Business Account** — created through Meta Business Suite
5. **Webhook endpoint** — FastAPI server with HTTPS (our GCP infrastructure)

### Setup steps:
1. Create Meta Business account
2. Create WhatsApp Business Account in Meta Business Suite
3. Register phone number (verify via SMS or call)
4. Set display name and business category
5. Wait for display name approval
6. Configure webhook URL pointing to our FastAPI server
7. Generate permanent access token
8. Start receiving and sending messages

### Timeline: Can be set up in 1-2 days once verified.

Sources:
- https://medium.com/@hamzas2401/how-i-registered-my-whatsapp-business-number-on-meta-b175a290a451
- https://callbellsupport.zendesk.com/hc/en-us/articles/13684222094620

---

## 6. How This Maps to ChatMRPT

### What stays the same:
- Entire backend (LangGraph, Gemini, Bayesian model, analysis tools, Earth Engine, OR-Tools)
- All ML models and processing
- Database, raster library, PostGIS
- GCP infrastructure

### What's new:
- **WhatsApp webhook endpoint** (FastAPI — we already plan to use FastAPI)
- **Message formatter** — converts ChatMRPT responses into WhatsApp-friendly format:
  - Text responses → WhatsApp text messages (chunk if >1,600 chars)
  - Maps/charts → render as PNG images, send via WhatsApp media API
  - Data tables → format as text or send as PDF/Excel document
  - Interactive choices → WhatsApp button/list messages
- **Session mapping** — map WhatsApp phone number to ChatMRPT session
- **Media handler** — receive uploaded Excel/CSV files via WhatsApp (up to 100MB)

### What this solves:
- **Low bandwidth:** WhatsApp handles compression, reconnection, queuing natively
- **Familiar interface:** Nigerian government officers already use WhatsApp daily
- **Asynchronous:** Users send a message and wait — no need for constant connection
- **File sharing:** They can upload data files directly via WhatsApp (up to 100MB for documents)
- **Mobile-first:** Works on any smartphone, no app install needed

---

## 7. Limitations & Considerations

1. **Interactive maps become static images** — no zooming/hovering on WhatsApp. Full interactive experience still requires the web app
2. **Message length limit (1,600 chars)** — long analysis responses need to be chunked or sent as PDF
3. **No rich HTML/markdown rendering** — responses are plain text + images
4. **Rate limits for new accounts** — start at 250 conversations/24hr, scales with quality
5. **Meta's policies** — automated messages must follow Meta's commerce and messaging policies
6. **Phone number dedicated** — can't use the same number for personal WhatsApp
7. **Web app still needed** — WhatsApp is the lightweight interface for common queries; complex analysis (full risk mapping workflow, multi-step microplanning) may still need the web app

---

## 8. Recommended Architecture for Diagram

Two interfaces, one backend:

```
                    ┌─────────────────┐
                    │   WhatsApp User  │
                    │ (mobile, low BW) │
                    └────────┬────────┘
                             │
                    WhatsApp Cloud API
                      (Meta webhook)
                             │
                    ┌────────▼────────┐
                    │  FastAPI Server  │◄──── Web App User
                    │  (webhook +     │      (React + PWA)
                    │   REST API)     │
                    └────────┬────────┘
                             │
              ┌──────────────▼──────────────┐
              │   Existing ChatMRPT Backend  │
              │  (LangGraph, Gemini, INLA,   │
              │   OR-Tools, Earth Engine,    │
              │   analysis tools, etc.)      │
              └──────────────┬──────────────┘
                             │
                    ┌────────▼────────┐
                    │    Outputs       │
                    │ (risk maps,      │
                    │  allocations,    │
                    │  insights)       │
                    └─────────────────┘
```

The key insight: **WhatsApp becomes a second frontend alongside the web app.** The backend is identical. The only new component is the WhatsApp integration layer (webhook + message formatter).

---

## 9. Python Libraries Available

| Library | What it does | Source |
|---------|-------------|--------|
| PyWa | Native FastAPI support, webhook-ready WhatsApp bot framework | https://pywa.readthedocs.io/ |
| wa_me | Modern Python wrapper for WhatsApp Cloud API | https://github.com/leandcesar/wa_me |
| py-whatsapp-cloudbot | Async Python bot library for WhatsApp Cloud API | https://github.com/ardatricity/py-whatsapp-cloudbot |

---

## 10. Key Takeaways for the Grant Proposal

1. **This is proven at scale in African health** — MomConnect serves 4.7M mothers in South Africa via WhatsApp
2. **Service conversations are FREE** — user-initiated queries cost nothing
3. **100MB document uploads** — users can share Excel files with malaria data directly via WhatsApp
4. **LangGraph + WhatsApp integration is documented** — Infobip has a production tutorial
5. **Minimal new engineering** — it's a new frontend layer, not a new backend
6. **Aligns with Google's goals** — solving the low-bandwidth problem they'll care about
7. **WhatsApp supports 60+ languages** including Hausa, Yoruba, Igbo (Nigeria's 3 major languages)
