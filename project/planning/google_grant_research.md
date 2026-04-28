# Google Grant - Tech Stack Research
**Date:** March 19, 2026
**Purpose:** Research findings to inform architecture diagram, cost breakdown, and proposal comments

---

## 1. Google's AI for Government Innovation Challenge

- **Funding:** $1M - $3M per organization (from $30M global fund)
- **What Google provides:** Funding + Cloud credits + pro bono technical support via multi-month Google.org Accelerator
- **Deadline:** April 3, 2026
- **Eligibility:** Nonprofits, social enterprises, academic institutions partnering with governments
- **Selection criteria:** Impact, Innovation, Feasibility, Scalability
- **Key quote:** Must "leverage generative or agentic AI applications that go beyond incremental change"
- **Google products highlighted:** Gemini Enterprise, Google Earth AI, Roads Management Insights, Vertex AI, NotebookLM
- **Previous health winner:** Jacaranda Health (SMS service for 3.8M mothers in Kenya)

**Implication:** They want to see Google products used. Our proposal should center on Gemini, Vertex AI, Earth Engine, and Earth AI.

---

## 2. Google Geospatial Reasoning

**What it actually is:**
- A framework for building agentic workflows, NOT a single model
- Python frontend with mapping/graphing/chat UI
- Agentic backend using LangGraph agents deployed on Vertex AI Agent Engine
- LLM-accessible tools for Earth Engine, BigQuery, Maps Platform, Cloud Storage

**Tech stack:**
- LLM: Gemini
- Agent framework: LangGraph (not LangChain directly — LangGraph is built on LangChain)
- Deployment: Vertex AI Agent Engine
- Infrastructure: Google Cloud Platform

**Foundation models included:**
- Population Dynamics Foundation Model (PDFM) — population behavior + environment
- Trajectory-based mobility foundation model
- Remote sensing models built on: Masked autoencoders, SigLIP, MaMMUT, OWL-ViT
- Trained on high-resolution satellite + aerial images with text descriptions and bounding boxes

**Data sources it accesses:**
- Earth Engine (satellite imagery)
- BigQuery
- Google Maps Platform
- Cloud Storage
- WeatherNext AI weather forecasts
- User's proprietary datasets

**Access:**
- TRUSTED TESTER PROGRAM ONLY — not publicly available
- Apply via Google form
- Initial testers: WPP, Airbus, Maxar, Planet Labs
- PDFM tested by 200+ organizations, expanding to UK, Australia, Japan, Canada, Malawi

**Relevance to us:**
- Architecturally similar to ChatMRPT (natural language → agentic tools → geospatial outputs)
- The grant could get us into the trusted tester program
- LangGraph + Vertex AI is the stack we should propose migrating to
- PDFM expanding to Malawi = health/Africa use cases are on their radar

---

## 3. Gemini API Pricing

### Free tier models (good for development):
- Gemini 2.5 Flash: FREE
- Gemini 2.5 Flash-Lite: FREE
- Gemini Embedding: FREE

### Paid tier (per 1M tokens):

| Model | Input | Output | Best for |
|-------|-------|--------|----------|
| Gemini 2.5 Pro | $1.25 (≤200k ctx) | $10.00 | Complex reasoning, our main conversational AI |
| Gemini 2.5 Flash | $0.30 | $2.50 | Fast responses, bulk analysis |
| Gemini 2.5 Flash-Lite | $0.10 | $0.40 | Simple tasks, high volume |
| Gemini 3.1 Pro Preview | $2.00 | $12.00 | Latest, most capable |

### Batch API: 50% discount on all rates

### Comparison to current OpenAI costs:
- GPT-4o: ~$2.50 input / $10.00 output per 1M tokens
- Gemini 2.5 Pro: $1.25 input / $10.00 output — roughly 50% cheaper on input
- Gemini 2.5 Flash: $0.30 / $2.50 — significantly cheaper for most queries

### Cost estimate for ChatMRPT (36 states rollout):
- Assume 1,000 conversations/month across all states
- Average conversation: ~5,000 input tokens, ~2,000 output tokens per exchange, ~10 exchanges
- Monthly tokens: 50M input + 20M output
- Using Gemini 2.5 Flash: ~$15 input + $50 output = ~$65/month = ~$780/year
- Using Gemini 2.5 Pro: ~$62.50 input + $200 output = ~$262/month = ~$3,150/year
- With grant cloud credits, this may be covered entirely

---

## 4. Google Earth Engine

- **FREE for academic/nonprofit use** (we qualify as academic institution)
- Commercial use requires paid Google Cloud tier
- Extensive satellite imagery catalog (Landsat, Sentinel, MODIS, etc.)
- Python API available (we already have a dormant client built)
- Usage quotas exist but are generally generous for research

**Cost: $0 for our use case** (academic nonprofit)

---

## 5. Tool Research

### PuLP (for building-level allocation optimization)
- **What:** Python LP/MILP optimizer for linear programming
- **Cost:** FREE (MIT license, open-source, COIN-OR project)
- **Solvers:** CBC included free; GLPK, CPLEX, GUROBI also supported
- **Feasibility:** High — standard tool for resource allocation optimization
- **Use case:** Given X nets, Y buildings, Z budget → optimize which buildings get how many nets

### Google OR-Tools
- **What:** Google's open-source optimization toolkit
- **Cost:** FREE (Apache 2.0 license)
- **Capabilities:** LP, MILP, constraint programming, vehicle routing, scheduling
- **Feasibility:** High — more feature-rich than PuLP, and it's a Google product (good for proposal)
- **Use case:** Same as PuLP but with more advanced features and Google branding

**Recommendation:** Use Google OR-Tools over PuLP — it's Google's own tool, free, and more capable. Better for the proposal.

### FAISS (Facebook AI Similarity Search)
- **What:** Vector database for storing/retrieving text embeddings for RAG
- **Cost:** FREE (MIT license, open-source by Meta)
- **Feasibility:** High — widely used, runs in-memory
- **Alternatives to consider:**
  - Vertex AI Vector Search (Google's managed offering — better for proposal)
  - ChromaDB (open-source, simpler)
  - pgvector (PostgreSQL extension)

**Recommendation:** Use Vertex AI Vector Search for the proposal — it's Google's managed service. FAISS as fallback/local development option.

### LangChain / LangGraph for RAG
- **LangChain:** Original orchestration framework for LLM applications
- **LangGraph:** Newer agent framework built on LangChain — THIS is what Google's Geospatial Reasoning uses
- **Cost:** FREE (open-source)
- **Current alternatives:** LlamaIndex, direct Gemini function calling
- **Note:** Google's own Geospatial Reasoning is built on LangGraph + Vertex AI

**Recommendation:** Propose LangGraph (not LangChain) — aligns directly with Google's own Geospatial Reasoning implementation.

### React Native for mobile
- **Cost:** FREE (open-source by Meta)
- **Alternative:** Progressive Web App (PWA) — works offline, no app store needed, built on existing React codebase
- **Consideration:** PWA is simpler to build from existing React frontend. React Native requires separate codebase.

**Recommendation:** Start with PWA for offline capability (less engineering effort), evaluate React Native later if needed. Mention both in proposal.

### MaxEnt / BioMod2 for Anopheles habitat modeling
- **MaxEnt:** Java-based species distribution model, FREE, standard in ecology
- **BioMod2:** R package, ensemble species distribution modeling, FREE
- **Vertex AI alternative:** Could train custom habitat model on Vertex AI AutoML
- **Data needed:** Presence/absence points for Anopheles species + environmental features

**Recommendation:** Use BioMod2 (R) or custom Vertex AI model. Ifeoma likely has experience with these in R.

---

## 6. GCP Infrastructure Cost Estimates

### Replacing AWS (current ~$10K/yr with credits):

| Service | AWS Current | GCP Equivalent | Est. Annual Cost |
|---------|-------------|----------------|------------------|
| EC2 (2 instances) | EC2 t3.medium | Compute Engine e2-medium | ~$3,000-5,000 |
| CloudFront CDN | CloudFront | Cloud CDN | ~$500-1,000 |
| Redis (ElastiCache) | ElastiCache | Memorystore for Redis | ~$2,000-3,000 |
| S3 (backups) | S3 | Cloud Storage | ~$200-500 |
| Load Balancer | ALB | Cloud Load Balancing | ~$500-1,000 |
| **Total infra** | | | **~$7,000-11,000/yr** |

### Additional GCP services for grant:

| Service | Purpose | Est. Annual Cost |
|---------|---------|------------------|
| Vertex AI (training) | ML model training (XGBoost, habitat model) | ~$2,000-5,000 |
| Vertex AI (serving) | Model inference endpoints | ~$1,000-3,000 |
| Vertex AI Vector Search | RAG vector store | ~$1,000-2,000 |
| Earth Engine | Satellite data processing | FREE (academic) |
| BigQuery | Data warehouse for analysis | ~$500-2,000 |
| Gemini API | Conversational AI | ~$800-3,200 |
| Cloud Storage (rasters) | 1.3GB+ growing raster library | ~$100-500 |
| **Total additional** | | **~$5,400-15,700/yr** |

### Grand total GCP estimate: ~$12,000-27,000/year

With Google Cloud credits from the grant, much of this may be covered. Budget $50K/yr gives substantial buffer for scaling to 36 states.

---

## 7. Proposed Tech Stack Summary (for diagram)

### Current Stack → Proposed Stack

| Layer | Current | Proposed |
|-------|---------|----------|
| **LLM** | OpenAI GPT-4o | Google Gemini 2.5 Pro/Flash |
| **Cloud** | AWS (EC2, CloudFront, Redis, S3) | GCP (Compute Engine, Cloud CDN, Memorystore, Cloud Storage) |
| **Agent Framework** | Custom Flask routing | LangGraph on Vertex AI Agent Engine |
| **Satellite Data** | Static rasters (1.3GB, manual refresh) | Google Earth Engine (dynamic, automated) |
| **ML Models** | Simple mean scoring + PCA | XGBoost/GBM on Vertex AI AutoML |
| **Habitat Model** | Does not exist | MaxEnt/BioMod2 or Vertex AI custom model |
| **RAG** | Does not exist | LangGraph + Vertex AI Vector Search + Gemini |
| **Optimization** | Does not exist | Google OR-Tools for building-level allocation |
| **Settlement Classification** | Does not exist | TensorFlow CNN on Vertex AI (using OpenBuildings) |
| **Vector Store** | Does not exist | Vertex AI Vector Search (FAISS for local dev) |
| **Frontend** | React (web only) | React (web) + PWA (offline mobile) |
| **Backend** | Flask | Flask + FastAPI (for ML serving endpoints) |

---

## 8. Roads Management Insights
- Google Maps Platform product for road network data
- REST API for route management and monitoring
- Relevant only if we need road/travel time analysis for catchment area modeling
- May be useful for Ifeoma's "how far will people travel to a facility" question
- Low priority — nice to have, not core

---

## Key Takeaways for Bernard

1. **Google wants to see their products.** Gemini, Vertex AI, Earth Engine, LangGraph, OR-Tools — use these.
2. **Gemini is cheaper than GPT-4o.** Migration saves money and aligns with funder.
3. **Earth Engine is FREE for us.** Biggest win — we can activate the dormant client at zero cost.
4. **Geospatial Reasoning is trusted-tester only.** The grant could be our pathway in.
5. **LangGraph is the right agent framework** — it's what Google uses internally.
6. **$50K/yr tech budget is generous** for our actual costs (~$12-27K/yr estimated).
7. **Deadline is April 3, 2026** — we have 2 weeks.
