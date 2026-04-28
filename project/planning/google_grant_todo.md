# Google Grant - Bernard's Action Items
**Date:** March 19, 2026
**Updated:** March 20, 2026
**Grant deadline:** April 3, 2026

---

## Status: ALL ITEMS COMPLETE

## 1. Research Tasks — DONE
- [x] All tools researched (Gemini, GEE, OR-Tools, FAISS, LangGraph, PuLP, React Native, BioMod2)
- [x] Full findings saved to `project/planning/google_grant_research.md`

## 2. Technical Architecture Diagram — DONE
- [x] Horizontal panel diagram matching Google's Geospatial Reasoning style
- [x] Delivered in `ChatMRPT_Technical_Architecture_Google_Grant.docx`

## 3. Tech Stack Cost Breakdown — DONE
- [x] Detailed cost estimates with citations in architecture docx
- [x] Budget-ready breakdown for Eniola: `ChatMRPT_Budget_Breakdown_For_Eniola.docx`
- [x] Organized into Google's budget categories (Technology Development + Infrastructure)
- [x] Total estimated: ~$12K-$26K/yr actual vs $50K/yr budgeted

## 4. Review Proposal & Add Comments — DONE
- [x] Updated review doc with research findings: `Bernard_Review_Google_AI_Grant.docx`
- [x] LangGraph/OR-Tools/Vertex AI recommendations incorporated
- [x] Meeting context added (catchment area modeling, prevalence-incidence, integer programming, NASDA, DSN Nigeria competitive threat)
- [x] Typos, number verification, and other issues documented

## 5. Housekeeping — DONE
- [x] Meeting transcript moved from repo root to `project/meetings/2026-03-19_meeting_transcript_raw.md`
- [x] Meeting notes at `project/meetings/2026-03-19_google_grant_budget_meeting.txt`

---

## Deliverables (all in Downloads folder)

| File | Purpose |
|------|---------|
| `ChatMRPT_Technical_Architecture_Google_Grant.docx` | Architecture diagram + cost breakdown + recommendations (for Ifeoma) |
| `Bernard_Review_Google_AI_Grant.docx` | Proposal review with suggestions (for team) |
| `ChatMRPT_Budget_Breakdown_For_Eniola.docx` | Budget numbers in Google's categories (for Eniola's Excel) |

## Key Recommendations Summary

| Decision | Recommendation |
|----------|---------------|
| LLM | Gemini 2.5 Pro/Flash (replaces GPT-4o) |
| Agent framework | LangGraph on Vertex AI Agent Engine |
| Optimization | Google OR-Tools (not PuLP) |
| Vector store | Vertex AI Vector Search (not FAISS) |
| Satellite data | Earth Engine (FREE for academic) |
| ML training | Vertex AI AutoML |
| Mobile | PWA first, React Native later |
| Habitat model | BioMod2 or Vertex AI custom |
| Cloud | GCP (migrate from AWS) |
