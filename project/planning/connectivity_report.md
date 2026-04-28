# ChatMRPT Connectivity Optimisation Report
**Project:** ChatMRPT — AI-Powered Malaria Surveillance System  
**Author:** Bernard Boateng  
**Date:** April 2026  
**Status:** Phase 2 Complete

---

## Section 1 — Nigeria Connectivity Landscape

### 1.1 Why Connectivity Matters for ChatMRPT

ChatMRPT is deployed to state-level malaria programme officers across Nigeria. These users operate from state, LGA, and facility offices — many of which are in semi-urban or peri-urban areas with limited infrastructure. The application requires users to upload data files (CSV/Excel and Shapefile archives), receive analysis results, and view visualisations. Each of these operations is directly affected by the quality and stability of the user's internet connection.

Designing for average Nigerian internet conditions is insufficient. The design target must be the *worst plausible conditions* that a real user in a target state will encounter.

---

### 1.2 National Speed Statistics — and Why They Mislead

Nigeria's national median mobile download speed is reported at **44.14 Mbps** (Speedtest Global Index, 2025, ranking Nigeria 85th globally). This figure is dominated by performance in Lagos, Abuja, Port Harcourt, and other major cities where 4G infrastructure is dense.

The figure relevant to ChatMRPT users is the **rural median: approximately 11 Mbps** — less than one quarter of the urban figure. This gap reflects Nigeria's coverage reality: while 4G coverage now reaches **84%** of the population (BusinessDay NG, 2024; GSMA Intelligence, 2024), coverage does not equal consistent throughput. Signal quality degrades sharply at the cell edge, inside buildings, and on moving vehicles — all common conditions for field staff.

| Context | Estimated Download Speed |
|---------|--------------------------|
| Lagos / Abuja (urban, 4G) | 20–40 Mbps |
| State capitals (semi-urban, 4G) | 8–15 Mbps |
| LGA towns (semi-urban, 3G/4G edge) | 1–5 Mbps |
| Rural / facility level (3G or EDGE) | 0.5–2 Mbps |

*Note: Specific measured speeds for Kwara, Kebbi, and Sokoto states are not publicly available in standardised databases. The figures above represent the known rural-urban gradient applied to northern states, which consistently lag southern benchmarks.*

---

### 1.3 Network Technology Distribution

According to GSMA Intelligence Sub-Saharan Africa 2024:
- **4G:** 50% of mobile connections nationally
- **3G:** 31% of connections
- **2G (EDGE/GPRS):** 2% of connections

However, connection type does not reflect quality of service. 4G coverage at the edge of a cell tower can deliver speeds equivalent to 3G or worse under load. More telling is this finding from a multi-state Nigerian primary health care study: **59.5% of PHC facilities lacked access to a 3G mobile network** (PLOS Digital Health, 2024). For a health application targeting facility-level users, this is the operative statistic.

---

### 1.4 NCC Quality of Service Thresholds

The Nigerian Communications Commission (NCC) published revised Quality of Service Regulations in 2024. The Nigerian National Broadband Plan 2020–2025 sets the following targets:
- **Urban minimum:** 25 Mbps download
- **Rural minimum:** 10 Mbps download
- **Minimum acceptable for data services:** 512 Kbps

The 512 Kbps figure is the practical floor that operators must meet. For a web application, 512 Kbps (64 KB/s) means that uploading a typical 7 MB dataset (2 MB CSV + 5 MB Shapefile ZIP) would take **~112 seconds** under these minimum conditions — well over the default 30-second HTTP timeout that most servers apply.

---

### 1.5 Why Standard HTTP Uploads Fail

A standard single-part HTTP POST upload has no recovery mechanism. If the connection drops at any point — at 10%, 50%, or 90% completion — the entire upload fails and must restart from zero.

At 1 Mbps on a 3G connection with 10–15% packet loss (documented in rural Sub-Saharan Africa), the probability of completing a 10 MB upload without interruption decreases significantly. The upload does not just slow down — it fails silently and the user sees no indication of what went wrong or how far along they were.

This is the core technical problem that the Phase 2 connectivity work addresses.

---

### 1.6 Precedent: How Other Health Tech Projects Have Handled This

**DHIS2 (Nigeria national deployment):**  
DHIS2 is Nigeria's national Health Management Information System, deployed in all 36 states. Its Android Capture application operates in **offline-first mode**: data is entered locally without internet, then synchronised automatically when a connection becomes available. Over 96% of facilities across three study states reported malaria data through DHIS2 (Data for Impact, 2023). The offline-first architecture is the established engineering response to poor connectivity in Nigerian health systems.

**NMEP / WHO Malaria Surveillance:**  
The National Malaria Elimination Programme uses DHIS2 Tracker with local device synchronisation. Field staff upload batched data when they return to areas with better connectivity — deliberately avoiding real-time dependency on network availability.

**Nigeria mHealth Studies:**  
Documented connectivity solutions across Nigerian mHealth programmes include: USSD-based low-bandwidth data entry, zero-rated data agreements with telecoms, satellite communication for remote facilities, and monthly internet data stipends for health workers (Nature npj Cardiovascular Health, 2025; GSMA Mobile for Development).

The consistent pattern is: **do not assume connectivity; design for intermittent, low-bandwidth conditions and make recovery automatic.**

---

## Section 2 — Phase 1: Baseline Connectivity Features (Already Implemented)

The following features were implemented in the initial ChatMRPT build to establish a connectivity-aware baseline:

### 2.1 Extended Upload Timeouts

**Files:** `app/web/routes/upload_routes.py`, `frontend/src/services/api.ts`

Standard HTTP and browser timeouts are 30 seconds — sufficient for fast connections, but not for uploads over slow mobile networks. ChatMRPT applies:
- **120,000 ms (2 minutes)** for upload API calls
- **30,000 ms** for standard analysis API calls

The 2-minute figure was chosen based on the calculation: a 7 MB upload at 512 Kbps (NCC minimum) takes ~112 seconds. The timeout must exceed the worst credible upload time with margin.

### 2.2 Direct File Streaming to Disk

**File:** `app/web/routes/upload_routes.py` — `store_raw_data_files()`

Uploaded files are streamed directly to disk without buffering the entire file in memory first. `os.sync()` is called after save to ensure durability. This is important because server memory is finite — buffering large files for multiple concurrent users would cause out-of-memory errors under load.

### 2.3 Snappy Compression on Analysis Output Files

**File:** `app/services/dataset_builder.py`

Analysis output files (GeoParquet format) are written with Snappy compression:
```python
gdf.to_parquet(..., compression='snappy')
```
Snappy reduces file sizes by 30–50% with negligible CPU overhead, improving both disk I/O and any subsequent file transfer operations.

### 2.4 Upload Progress Spinner

**File:** `frontend/src/components/Modal/UploadModal.tsx`

A loading spinner with "Uploading..." text is displayed during upload to prevent users from closing the modal or re-submitting. This prevents duplicate upload requests on slow connections where users assume the upload has stalled.

---

## Section 3 — Phase 2: Advanced Connectivity Features (Implemented April 2026)

### 3.1 Chunked / Resumable Uploads

**Files:** `app/api/upload_routes.py` (new endpoints), `frontend/src/components/Modal/UploadModal.tsx`, `frontend/src/services/api.ts`

**Problem:** On a 1 Mbps connection, uploading a 10 MB dataset takes ~80 seconds. A single connection drop at any point restarts the entire upload from zero. On Nigerian 3G networks with intermittent connectivity, this makes large uploads unreliable.

**Solution:** Files larger than **5 MB** (combined CSV + Shapefile) are automatically split into **1 MB chunks** on the client. Each chunk is uploaded independently via `POST /upload/chunk`. When all chunks have been received, the client calls `POST /upload/finalize`, which reassembles the file on the server and runs the standard processing pipeline.

**Recovery:** Each chunk is retried independently on failure. If a connection drops mid-upload, only the current 1 MB chunk needs to be re-sent — not the entire file. The effective maximum data loss on a connection drop is 1 MB regardless of total file size.

**New API endpoints:**
- `POST /upload/chunk` — receives a single chunk with `upload_id`, `file_type`, `chunk_index`, `total_chunks`
- `POST /upload/finalize` — assembles chunks, creates a session, runs the processing pipeline

Files below 5 MB continue to use the existing single-part upload path, preserving backward compatibility and avoiding unnecessary overhead for small files.

### 3.2 Gzip HTTP Response Compression

**Files:** `app/__init__.py`, `requirements.txt`

**Problem:** ChatMRPT API responses — particularly analysis results containing ward-level data — can range from 10 KB to 200 KB+ of JSON. On a 1 Mbps connection, a 100 KB response takes ~0.8 seconds; at 512 Kbps, ~1.6 seconds. Across an analysis session with multiple API calls, this adds up to perceptible latency.

**Solution:** Flask-Compress (`flask-compress==1.15`) is initialised in the application factory and automatically applies Gzip compression to all qualifying HTTP responses (JSON, HTML, JavaScript).

**Measured compression ratios (actual ChatMRPT response payloads):**

| Payload | Uncompressed | Gzip Compressed | Reduction |
|---------|-------------|-----------------|-----------|
| Analysis result (50 wards) | 11.5 KB | 1.3 KB | **88.7%** |
| Full ward dataset (193 wards) | 50.8 KB | 5.6 KB | **89.1%** |

A 193-ward dataset response that would take **0.4 seconds** at 1 Mbps uncompressed now takes **0.05 seconds** compressed. The browser decompresses automatically — no client-side changes required.

### 3.3 Real Percentage Progress Bar

**Files:** `frontend/src/components/Modal/UploadModal.tsx`, `frontend/src/services/api.ts`

**Problem:** The Phase 1 spinner provides no information about upload progress. On a slow connection where an upload takes 60–90 seconds, users cannot distinguish between a slow upload and a stalled or failed one. In user studies of health applications in low-resource settings, absence of feedback is consistently cited as a cause of premature session abandonment.

**Solution:** The upload spinner is replaced with a real percentage progress bar driven by the axios `onUploadProgress` event. The bar fills in real time as bytes are transmitted. Status text updates from `"Uploading... 47%"` to `"Processing..."` when the file transfer completes and server-side processing begins.

For chunked uploads, progress reflects the proportion of total bytes (across both files) successfully sent to the server, giving an accurate representation across the multi-chunk sequence.

### 3.4 Retry Logic with Exponential Backoff

**File:** `frontend/src/services/api.ts` — `withRetry()` utility function

**Problem:** On flaky 3G connections, a transient network error (dropped packet, brief signal loss) causes the upload request to fail immediately with a network error. The user sees an error message and must manually retry from scratch, including re-selecting their files.

**Solution:** A `withRetry()` wrapper is applied to all upload API calls. On a network error or 5xx server response, the request is automatically retried up to **3 times** with exponential backoff:

| Attempt | Wait before retry |
|---------|------------------|
| 1st retry | 1 second |
| 2nd retry | 2 seconds |
| 3rd retry | 4 seconds |

The user sees `"Connection issue, retrying... (1/3)"` in the upload status bar — maintaining transparency without requiring manual intervention.

**Critically:** 4xx errors (bad request, authentication failure) are **not** retried. These indicate a client-side problem that will not resolve with retrying, and retrying them would create unnecessary load on the server.

---

## Section 4 — Benchmarks

### 4.1 Theoretical Upload Time: Before vs After Chunked Uploads

Scenario: User uploads a 5 MB Shapefile ZIP + 2 MB CSV = **7 MB total**

| Connection Speed | Standard Upload (single-part) | Chunked Upload (1 MB chunks) |
|----------------|-------------------------------|------------------------------|
| 512 Kbps (NCC minimum) | 112s (likely fails on drop) | 112s; any drop retries ≤8s |
| 1 Mbps | 57s (likely fails on drop) | 57s; any drop retries ≤8s |
| 3 Mbps | 19s | 19s |
| 11 Mbps (rural median) | 5s | 5s |

The chunked approach does not reduce total transfer time — it eliminates the penalty for connection drops. On a standard upload, a drop at 90% completion means restarting all 7 MB. On a chunked upload, the same drop means resending at most the current 1 MB chunk.

### 4.2 Gzip Response Compression: Before vs After

| Response Type | Uncompressed | Compressed | Time at 1 Mbps (before) | Time at 1 Mbps (after) |
|--------------|-------------|-----------|--------------------------|------------------------|
| Analysis result (50 wards) | 11.5 KB | 1.3 KB | 0.09s | 0.01s |
| Full ward dataset (193 wards) | 50.8 KB | 5.6 KB | 0.41s | 0.04s |

*Compression ratios measured on actual ChatMRPT JSON response payloads. Gzip level 6 (Flask-Compress default).*

Across a full analysis session involving 5–10 API calls, the cumulative saving at 1 Mbps is approximately **2–3 seconds** — meaningful on a mobile connection where each round-trip already carries latency overhead.

---

## Section 5 — Recommendations

### 5.1 Immediate Next Steps

**PWA Service Worker Caching:** The application currently requires a live connection to load the React frontend (~157 KB gzipped). A Progressive Web App service worker could cache the application shell on first load, allowing the UI to load instantly on subsequent visits even without a network connection. This is particularly valuable for users in intermittent coverage areas.

**Offline Data Entry:** The most impactful long-term feature — analogous to DHIS2's offline-first architecture — would be allowing users to view previously loaded data and run analyses that do not require a server round-trip. Analysis results and maps could be cached locally and remain accessible when the connection drops.

**Server-Sent Events for Long Analyses:** Current analysis requests block on a single HTTP response. For analyses that take 10–30 seconds on the server, this creates a long wait with no feedback. Server-Sent Events or WebSocket streaming would allow partial results to be pushed to the client progressively.

### 5.2 WhatsApp as a Low-Bandwidth Interface

The connectivity challenges described in this report — intermittent connections, upload failures, long response times — are intrinsic to the HTTP-based web application model. WhatsApp offers a fundamentally different communication channel that sidesteps many of these issues.

WhatsApp uses asynchronous messaging: the user sends a message when connected; the response arrives when the server has processed it; the user reads it when they next have a connection. There is no maintained HTTP connection to drop. WhatsApp handles compression, reconnection, and message queuing natively.

A WhatsApp integration (planned for delivery by **30 May 2026**) would allow state malaria officers to query ChatMRPT via WhatsApp — asking questions, requesting summaries, and uploading data files — without requiring a sustained browser session. This is particularly relevant for field-based users who access the system on mobile data. Research shows MomConnect served 4.7 million mothers in South Africa via WhatsApp, demonstrating the viability of health data delivery through the platform at scale.

The WhatsApp integration is designed as an additive frontend layer — the existing ChatMRPT backend and analysis pipeline are unchanged.

---

## References

- GSMA Intelligence. *The Mobile Economy: Sub-Saharan Africa 2024*. GSMA, 2024.
- BusinessDay NG. "Nigeria's 4G Coverage Grows to 84% in 2024." 2024.
- Speedtest Global Index. Nigeria Mobile Performance, Q4 2025. Ookla, 2025.
- NCC. *Nigerian National Broadband Plan 2020–2025*. Nigerian Communications Commission, 2020.
- NCC. *Quality of Service Regulations 2024*. Nigerian Communications Commission, 2024.
- Data for Impact. *Nigeria DHIS2 Evaluation Results Brief*. D4I/USAID, 2023.
- PLOS Digital Health. "Integrating Digital Health Technologies in Nigeria." 2024.
- PLOS Neglected Tropical Diseases. "DHIS2 Treatment Reporting for Mass Drug Administration in Nigeria." 2024.
- GSMA Mobile for Development. *MomConnect Case Study: Evidence for Scale*. GSMA, 2021.
- Nature npj Cardiovascular Health. "Digital Health Technology in Sub-Saharan Africa." 2025.
