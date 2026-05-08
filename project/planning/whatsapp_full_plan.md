# WhatsApp Integration — Corrected Full Plan
**Date:** May 2026  
**Branch:** whatsapp-integration  
**Version:** 5.2 (final review fixes before implementation)

---

## Executive Decision

This integration is feasible, but the previous v4 plan was architecturally wrong in one
important way: WhatsApp must not call `DataAnalysisAgent(session_id).analyze_stream()`
directly for all messages.

`DataAnalysisAgent` is the LangGraph analysis engine, but the production web experience
also depends on route-level orchestration in `app/api/data_analysis_routes.py`:

- TPR workflow start
- active TPR workflow command handling
- workflow-stage persistence
- schema restoration through `DataAnalysisStateManager`
- guided TPR selection/confirmation behavior
- fallback to flexible data analysis agent

Therefore, the corrected plan is:

1. Extract the production data-analysis orchestration from Flask routes into reusable
   service functions.
2. Make the web routes and WhatsApp route call the same service.
3. Extract the production upload processing into a reusable service.
4. Make web uploads and WhatsApp uploads produce the same artifacts and state.
5. For v1, keep the existing `app/services/instance_sync.py` rsync mechanism for
   private cross-instance session files.
6. Use S3 only for public WhatsApp map links under `maps/*`.
7. Keep Twilio webhooks fast, idempotent, and retry-safe.

WhatsApp becomes a transport channel. The backend workflow remains shared.

---

## Current Production Facts

Confirmed from the codebase:

- `DataAnalysisAgent` lives in `app/agent/agent.py`.
- `DataAnalysisAgent.analyze_stream(message)` is synchronous and yields:
  - `{'type': 'thinking', 'content': ...}`
  - `{'type': 'result', 'data': {...}}`
  - `{'type': 'error', 'error': ...}`
- `DataAnalysisAgent` is not the full TPR workflow controller. Its file header says it
  contains no TPR workflow logic.
- Production TPR orchestration currently lives in `app/api/data_analysis_routes.py`,
  especially `_handle_tpr_start()` and `_handle_tpr_active()`.
- `data_loader.get_input_data(session_id)` reads from `instance/uploads/{session_id}/`.
- Main file priority is:
  - post-analysis: `unified_dataset.csv` -> `raw_data.csv` -> `tpr_results.csv` ->
    `data_analysis.csv` -> `uploaded_data.csv`
  - pre-analysis: `raw_data.csv` -> `tpr_results.csv` -> `data_analysis.csv` ->
    `uploaded_data.csv`
- `MetadataCache.update_file_metadata(session_id, filepath, filename)` is the correct
  metadata method. It extracts metadata and saves `metadata_cache.json`.
- `UploadResult` has `.saved_path` and `.original_filename`, plus optional
  `.alias_path` and `.alias_filename`.
- New S3 buckets block ACLs by default. Do not use `ACL='public-read'`.
- CloudFront currently serves EC2/ALB only, not S3. v1 map links use direct S3 URLs.
- Private cross-instance session file sharing already exists in
  `app/services/instance_sync.py` using rsync over SSH.
- The web data-analysis upload route already calls
  `sync_session_after_upload(session_id)`.
- The sync chat route already calls `ensure_session_available(session_id)` before
  analysis.
- `get_memory_service()` returns a `MemoryService`. Redis backing is best-effort when
  `CHATMRPT_USE_REDIS_MEMORY=1`; if Redis memory fails, it silently falls back to local
  files unless this is hardened.

---

## Target Architecture

```
                           ┌──────────────────────────────┐
Web UI upload ───────────▶ │ shared upload service         │
WhatsApp upload ─────────▶ │ app/services/analysis_upload  │
                           └──────────────┬───────────────┘
                                          │
                                          ▼
                           instance/uploads/{session_id}/
                           - original upload
                           - data_analysis.{ext}
                           - uploaded_data.csv
                           - cleaning_report.json
                           - metadata_cache.json
                           - .data_analysis_mode
                           - DataAnalysisStateManager state
                                          │
                                          ▼
                           existing instance_sync rsync


                           ┌──────────────────────────────┐
Web chat route ──────────▶ │ shared analysis service       │
WhatsApp text ───────────▶ │ app/services/analysis_chat    │
                           └──────────────┬───────────────┘
                                          │
                         ┌────────────────┴────────────────┐
                         ▼                                 ▼
              TPR workflow start/active          DataAnalysisAgent fallback
              app.tpr.workflow_manager           app.agent.agent
```

The web route and WhatsApp route must not maintain duplicate workflow logic.

Storage decision for v1:

- Keep `instance_sync.py` for private session files.
- Do not migrate private upload/session state to S3 in this phase.
- Use S3 only for public visualization HTML links sent to WhatsApp users.
- A later migration may replace rsync with S3 or EFS, but that is not part of this
  implementation plan.

Route boundary decision:

- The shared `analysis_chat_service` is extracted from Data Analysis V3 endpoints in
  `app/api/data_analysis_routes.py` first:
  - `/api/v1/data-analysis/chat`
  - `/api/v1/data-analysis/chat/stream`
  - helpers such as `_handle_tpr_start()` and `_handle_tpr_active()`
- Do not refactor the general `/send_message` handlers in `app/api/analysis/` as part
  of this WhatsApp phase. Those routes may continue to call the Data Analysis V3
  endpoint/service indirectly as they do today.

---

## New/Refactored Services

### 1. `app/services/analysis_upload_service.py`

Purpose: centralize the existing upload semantics from
`app/api/data_analysis_routes.py:/api/data-analysis/upload`.

Public API:

```python
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class AnalysisUploadResult:
    session_id: str
    original_filename: str
    saved_path: Path
    standard_path: Path
    uploaded_csv_path: Path | None
    rows: int
    cols: int
    detected_type: str
    key_columns: list[str]
    column_schema: dict[str, Any]
    cleaning_report: Any | None
    metadata: dict[str, Any] | None
    error: str | None = None


def process_analysis_upload(
    *,
    session_id: str,
    file_obj,
    original_filename: str,
    upload_root: str,
) -> AnalysisUploadResult:
    ...
```

`file_obj` may be a Werkzeug `FileStorage`, a file-like object, or a bytes wrapper.
The service should stream/save from the object when possible. Do not force the web
upload route to read the entire upload into memory.

Required behavior:

1. Sanitize `original_filename` with `secure_filename`.
2. Validate extension: `.csv`, `.xlsx`, `.xls`, `.json`, `.txt` for web parity.
   WhatsApp should restrict to `.csv`, `.xlsx`, `.xls` at the route level unless we
   intentionally support JSON/TXT there.
3. Save the original file in `upload_root/{session_id}/{safe_name}`.
4. Copy to standard name:
   - CSV -> `data_analysis.csv`
   - Excel -> `data_analysis.xlsx`
   - JSON -> `data_analysis.json`
   - text -> `data_analysis.txt`
5. Run `TPRDataAnalyzer().infer_schema_from_file(original_path)` when the file is CSV
   or Excel.
6. Run DHIS2 cleaning using `clean_dhis2_export(df, mode=get_cleaner_mode())`.
7. If cleaning returns `column_rename_map`, update the schema with
   `apply_rename_map_to_schema(schema, column_rename_map)`.
8. Save normalized `uploaded_data.csv`.
9. Save `cleaning_report.json` when cleaner runs.
10. Call `MetadataCache.update_file_metadata()` for:
    - original file
    - standard file
    - `uploaded_data.csv` when present
11. Write `.data_analysis_mode`.
12. Update `DataAnalysisStateManager(session_id)` with:
    - `workflow_transitioned: False`
    - `tpr_completed: False`
    - `column_schema: schema_at_upload`
13. Run `WorkflowStateManager.transition_workflow(...)` exactly as the web upload
    path does today.
14. Return rows/cols/key columns from `uploaded_data.csv` when available.

Important: This service must use `current_app.config['UPLOAD_FOLDER']` or an explicit
`upload_root` passed from the caller. Do not hardcode `app.instance_path` in this
service because the production app config already defines the upload folder.

Physical path constraint:

- Until `DataAnalysisStateManager`, `data_loader`, TPR workflow code, and
  `instance_sync.py` are made fully configurable, `upload_root` must resolve to the
  same physical directory as the existing app's `instance/uploads`.
- On EC2, this must be `/home/ec2-user/ChatMRPT/instance/uploads` because
  `instance_sync.py` checks that hardcoded location.
- The upload service must always write `.data_analysis_mode`; `ensure_session_available()`
  currently uses that marker to decide whether a remote session exists.

Request-context boundary:

- This service must not read or write Flask `session`.
- The web route remains responsible for browser/frontend session flags such as
  `has_data_analysis_file`, `data_analysis_filename`, `workflow_source`,
  `workflow_stage`, `use_data_analysis_v3`, `csv_loaded`, `data_analysis_active`, and
  `active_tab`.
- WhatsApp has no Flask browser session. It uses Redis keys managed by
  `WhatsAppSessionManager` instead.
- This service may use `current_app` only when the caller has provided an app context.
  Background callers must wrap execution in `with app.app_context():`.

Migration step:

- Refactor `/api/data-analysis/upload` to call `process_analysis_upload()` instead of
  duplicating the logic in the route.
- Preserve the current route response shape so the frontend does not change.

---

### 2. `app/services/analysis_chat_service.py`

Purpose: centralize the production data-analysis message orchestration currently in
`app/api/data_analysis_routes.py`.

Public API:

```python
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass
class AnalysisChatResult:
    success: bool
    message: str
    session_id: str
    workflow: str | None = None
    stage: str | None = None
    visualizations: list[dict[str, Any]] | None = None
    error: str | None = None


def run_analysis_message(
    *,
    session_id: str,
    message: str,
    stream: bool = False,
) -> AnalysisChatResult | Iterable[dict[str, Any]]:
    ...
```

Required behavior:

1. Load `DataAnalysisStateManager(session_id)`.
2. Detect active TPR workflow using the same state checks as the current web routes.
3. If TPR workflow is active, call the extracted equivalent of `_handle_tpr_active()`.
4. If the message starts the TPR workflow, call the extracted equivalent of
   `_handle_tpr_start()`.
5. Otherwise call `DataAnalysisAgent(session_id)` for flexible analysis.
6. Preserve `workflow_context` behavior when the active TPR workflow routes a question
   to the agent.
7. Return a structured `AnalysisChatResult` for sync callers.
8. For stream callers, yield the same event shapes currently returned by the SSE route.

Migration step:

- Move helper logic out of `app/api/data_analysis_routes.py` into this service.
- Leave the Flask route as request parsing and response formatting only.
- WhatsApp must call this service, not `DataAnalysisAgent` directly.

---

### 3. Existing `app/services/instance_sync.py`

Purpose: private cross-instance session-file availability.

This already exists and is wired into the web app:

- `sync_session_after_upload(session_id)` pushes a session directory to the other EC2
  instances after upload.
- `ensure_session_available(session_id)` checks local disk and pulls the session from
  another instance when needed.

WhatsApp v1 must reuse these functions:

- After a successful WhatsApp upload, call `sync_session_after_upload(session_id)`.
- Before WhatsApp analysis, call `ensure_session_available(session_id)`.

Do not add private S3 sync for `uploads/{session_id}/...` in v1. That would be a
separate storage migration and should be planned independently.

Marker requirement:

- `ensure_session_available()` only treats a remote session as recoverable when
  `.data_analysis_mode` exists in the session directory.
- If that marker is missing, cross-instance recovery can silently fail even when data
  files exist.
- Therefore the upload service must create `.data_analysis_mode` before calling
  `sync_session_after_upload(session_id)`.

Known limitation:

- `instance_sync.py` is tied to EC2 private IPs/SSH/rsync. It is acceptable for this
  v1 because the existing production web app already depends on it. A future storage
  upgrade should compare S3, EFS, and current rsync.

---

### 4. `app/utils/s3_map_storage.py`

Purpose: public map publishing for WhatsApp links.

Rules:

- Read env vars lazily inside functions.
- Do not use object ACLs.
- Direct public map access is via S3 bucket policy for `maps/*`.
- Upload public HTML visualizations under `maps/{session_id}/...`.
- URL-escape S3 keys when returning public URLs.

Required API:

```python
def upload_public(local_path: str, s3_key: str) -> str | None:
    """Upload public map HTML and return direct S3 or CloudFront URL."""
```

Public URL construction:

```python
from urllib.parse import quote


def _quote_key(key: str) -> str:
    return "/".join(quote(part, safe="") for part in key.split("/"))
```

IAM for v1 can be narrow:

```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject"],
  "Resource": "arn:aws:s3:::chatmrpt-uploads/maps/*"
}
```

`s3:ListBucket` and `s3:DeleteObject` are not required for v1 map publishing.

---

### 5. `app/whatsapp/media.py`

Purpose: authenticated Twilio media download.

```python
import os
import logging
import requests

logger = logging.getLogger(__name__)


def download_twilio_media(media_url: str) -> tuple[bytes, str]:
    if not media_url:
        raise ValueError("Missing Twilio media URL")

    resp = requests.get(
        media_url,
        auth=(os.getenv("TWILIO_ACCOUNT_SID", ""), os.getenv("TWILIO_AUTH_TOKEN", "")),
        timeout=60,
        stream=True,
    )
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()
    content_length = int(resp.headers.get("Content-Length") or 0)
    max_bytes = int(os.getenv("WHATSAPP_MAX_UPLOAD_BYTES", str(32 * 1024 * 1024)))
    if content_length and content_length > max_bytes:
        raise ValueError("File is too large for WhatsApp upload processing")

    chunks = []
    total = 0
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise ValueError("File is too large for WhatsApp upload processing")
        chunks.append(chunk)

    file_bytes = b"".join(chunks)
    logger.info("Downloaded Twilio media: %s bytes, type=%s", len(file_bytes), content_type)
    return file_bytes, content_type
```

`requests` must be added explicitly to `requirements.txt`.

---

### 6. `app/whatsapp/responder.py`

Purpose: send analysis service output to WhatsApp.

This module must call `run_analysis_message()`, not `DataAnalysisAgent` directly.

```python
from pathlib import Path
from app.services.analysis_chat_service import run_analysis_message
from app.services.instance_sync import ensure_session_available, sync_session_after_upload
from app.utils import s3_map_storage
from app.whatsapp.formatter import chunk_text, format_error


def resolve_visualization_file(viz: dict, session_id: str, upload_root: str) -> Path | None:
    raw_path = viz.get("file_path") or viz.get("path")
    if raw_path:
        candidate = Path(raw_path)
        if not candidate.is_absolute():
            candidate = Path(upload_root) / session_id / raw_path
        return candidate if candidate.exists() else None

    url = viz.get("url", "")
    marker = f"/serve_viz_file/{session_id}/"
    if marker in url:
        rel = url.split(marker, 1)[1].split("?", 1)[0]
        candidate = Path(upload_root) / session_id / rel
        return candidate if candidate.exists() else None

    filename = url.split("/")[-1].split("?", 1)[0]
    if filename:
        for rel_dir in ("visualizations", ""):
            candidate = Path(upload_root) / session_id / rel_dir / filename
            if candidate.exists():
                return candidate
    return None


def run_whatsapp_analysis_and_respond(
    *,
    user_message: str,
    sender: str,
    session_id: str,
    send_fn,
    app,
) -> None:
    with app.app_context():
        upload_root = app.config.get("UPLOAD_FOLDER", str(Path(app.instance_path) / "uploads"))

        ensure_session_available(session_id)

        try:
            result = run_analysis_message(
                session_id=session_id,
                message=user_message,
                stream=False,
            )
        except Exception:
            send_fn(sender, [format_error()], app)
            raise

        if not result.success:
            send_fn(sender, [f"Warning: {result.error or result.message or format_error()}"], app)
            return

        if result.message:
            send_fn(sender, chunk_text(result.message), app)
        else:
            send_fn(sender, ["Analysis complete. Ask me a follow-up question."], app)

        for viz in result.visualizations or []:
            local_path = resolve_visualization_file(viz, session_id, upload_root)
            if not local_path:
                continue

            filename = local_path.name
            public_url = s3_map_storage.upload_public(
                str(local_path),
                f"maps/{session_id}/{filename}",
            )
            if public_url:
                title = viz.get("title", "Map")
                send_fn(sender, [f"{title}\n{public_url}"], app)

        sync_session_after_upload(session_id)
```

Note: The example omits emojis intentionally for ASCII compatibility in the plan. The
implementation may keep existing WhatsApp emoji strings if desired.

Conversation history:

- The WhatsApp route or responder must append `wa_history` entries after processing:
  - `mgr.append_history(sender, "user", user_message)`
  - `mgr.append_history(sender, "assistant", result.message or fallback_text)`
- This is separate from `MemoryService`, which is keyed by `session_id` and used by
  the analysis agent.

---

## WhatsApp Session Manager Changes

Current `WhatsAppSessionManager` already maps phone -> session and stores history.

Add upload metadata:

```python
def set_upload_metadata(self, phone: str, metadata: dict) -> None:
    self.redis.setex(f"wa_upload:{phone}", _TTL, json.dumps(metadata))


def get_upload_metadata(self, phone: str) -> dict | None:
    raw = self.redis.get(f"wa_upload:{phone}")
    if not raw:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None
```

Update clear:

```python
def clear_session(self, phone: str) -> None:
    self.redis.delete(
        f"wa_session:{phone}",
        f"wa_history:{phone}",
        f"wa_upload:{phone}",
    )
```

Add an atomic idempotency helper:

```python
def claim_message(self, message_sid: str, ttl: int = 120) -> bool:
    if not message_sid:
        return True
    key = f"wa_idem:{message_sid}"
    claimed = self.redis.set(key, "1", ex=ttl, nx=True)
    return bool(claimed)
```

This replaces `get()` followed by `setex()`.

The local in-memory fallback used in development must implement compatible `set(...,
ex=None, nx=False)`.

Development fallback implementation:

```python
import time


def _purge_if_expired(self, key):
    expires_at = self._ttls.get(key)
    if expires_at is not None and expires_at <= time.time():
        self._store.pop(key, None)
        self._ttls.pop(key, None)


def get(self, key):
    self._purge_if_expired(key)
    return self._store.get(key)


def set(self, key, value, ex=None, nx=False):
    self._purge_if_expired(key)
    if nx and key in self._store:
        return False
    self._store[key] = value
    if ex is not None:
        self._ttls[key] = time.time() + ex
    return True
```

This keeps local development from crashing when `claim_message()` uses Redis-style
atomic `SET NX EX`, and makes duplicate-message tests with TTL expiry meaningful.

---

## Corrected WhatsApp Webhook Flow

### File upload

```
Twilio POST /api/whatsapp/webhook
  ├─ Validate Twilio signature
  ├─ Defensively parse sender, MessageSid, NumMedia, MediaUrl0, MediaContentType0, filename
  ├─ If MessageSid is missing -> return empty TwiML 200 and log warning
  ├─ Claim/process via wa_job:{MessageSid} state machine
  ├─ If job status is processing/succeeded -> return empty TwiML 200
  ├─ If job status is failed and retryable -> claim again
  ├─ Validate MediaUrl0 exists
  ├─ Validate extension/content type before rotating session
  ├─ Return empty TwiML 200 quickly
  └─ Background task:
       a. Send "Received file. Processing..."
       b. Download Twilio media with Basic Auth
       c. Re-check size/type
       d. Create fresh session_id
       e. Set wa_session:{sender}
       f. Delete wa_history:{sender}, wa_upload:{sender}
       g. process_analysis_upload(...)
       h. sync_session_after_upload(session_id)
       i. mgr.set_upload_metadata(...)
       j. Send upload summary or error
       k. Set wa_job:{MessageSid} to succeeded/failed
```

Important: Do not rotate the sender to a new session until the media is known to be
downloadable and processable enough to start the upload.

### Text analysis

```
Twilio POST /api/whatsapp/webhook
  ├─ Validate Twilio signature
  ├─ Defensively parse sender/body/MessageSid/NumMedia
  ├─ If MessageSid is missing -> return empty TwiML 200 and log warning
  ├─ Claim/process via wa_job:{MessageSid} state machine
  ├─ If job status is processing/succeeded -> return empty TwiML 200
  ├─ If job status is failed and retryable -> claim again
  ├─ session_id = mgr.get_or_create_session(sender)
  ├─ Return empty TwiML 200 quickly
  └─ Background task:
       a. Send "Running analysis..."
       b. ensure_session_available(session_id)
       c. run_analysis_message(session_id, body)
       d. Send chunked text
       e. Upload visualization HTML to maps/{session_id}/...
       f. Send map links
       g. sync_session_after_upload(session_id)
       h. Append wa_history user/assistant turns
       i. Set wa_job:{MessageSid} to succeeded/failed
```

This preserves the production TPR behavior because `run_analysis_message()` owns the
same routing decisions as the web route.

Webhook parsing must avoid direct `int(request.form.get("NumMedia", 0))`; malformed
Twilio form values should be treated as zero media with a warning, not a 500.

Job/idempotency state:

- `wa_idem:{MessageSid}` or `wa_job:{MessageSid}` must not permanently suppress useful
  retries after a background failure.
- Minimum thread-based v1 should store JSON under `wa_job:{MessageSid}` with
  `status`, `sender`, `session_id`, `started_at`, `finished_at`, and `error`.
- Duplicates with `processing` or `succeeded` are ignored.
- Duplicates with `failed` may retry when the failure is retryable.
- A durable queue remains the preferred production implementation.

---

## Background Execution

Minimum v1 implementation may keep background threads if needed for speed of delivery,
but the production-safe target is a durable queue.

Recommended production path:

- Add RQ or Celery using the existing ElastiCache Redis.
- Enqueue `process_whatsapp_upload_job(...)`.
- Enqueue `process_whatsapp_analysis_job(...)`.
- Store job status in Redis under `wa_job:{MessageSid}` or `wa_job:{job_id}`.
- Ensure jobs are idempotent by `MessageSid`.

If threads are used temporarily:

- Never mark a task complete before final Twilio send.
- Log exceptions with `MessageSid`, sender, and session_id.
- Persist enough state that a duplicate Twilio webhook can detect an in-progress task.
- Avoid long blocking work before returning TwiML 200.

---

## AWS Prerequisites

### S3 bucket

Create `chatmrpt-uploads` in `us-east-2`.

This bucket is for public WhatsApp map links only in v1. Private session files remain
on EC2 local disk and are shared by `app/services/instance_sync.py`.

Public map files:

```
maps/{session_id}/...
```

Bucket policy for maps only:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::chatmrpt-uploads/maps/*"
  }]
}
```

Do not use object ACLs.

Block Public Access:

- Keep ACL-related block settings enabled.
- Disable only the public-policy blocking settings required for the `maps/*` bucket
  policy to work.
- Confirm with AWS console/CLI because account-level Block Public Access can override
  bucket-level settings.

IAM role permissions for v1 map publishing:

```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject"],
  "Resource": "arn:aws:s3:::chatmrpt-uploads/maps/*"
}
```

Environment variables on both EC2 instances:

```
S3_UPLOADS_BUCKET=chatmrpt-uploads
CHATMRPT_USE_REDIS_MEMORY=1
```

`S3_UPLOADS_PREFIX` is not needed in v1 because private session files stay on
`instance_sync.py`; S3 is only used for `maps/*`.

Hardening requirement:

- In production, if `CHATMRPT_USE_REDIS_MEMORY=1` but Redis memory cannot initialize,
  log an error clearly. Prefer failing startup or disabling WhatsApp analysis rather
  than silently using local memory across two EC2 instances.
- Required code change: add a strict mode for WhatsApp/background analysis. When
  `CHATMRPT_USE_REDIS_MEMORY=1` and the Redis-backed memory manager cannot read/write,
  raise a configuration/runtime error instead of falling back to local JSON files for
  WhatsApp requests.
- Acceptable implementation options:
  - add `CHATMRPT_REDIS_MEMORY_STRICT=1` and make `MemoryService` raise during
    initialization or first Redis read/write failure;
  - or add a WhatsApp startup/check helper that verifies Redis memory before enabling
    WhatsApp analysis.

CloudFront:

- Do not use CloudFront for maps in v1.
- Direct S3 URL format:
  `https://chatmrpt-uploads.s3.us-east-2.amazonaws.com/maps/...`
- Later, add S3 as a second CloudFront origin and set `CLOUDFRONT_DOMAIN`.

---

## Requirements

Add explicit direct dependencies:

```
boto3>=1.34.0
requests>=2.32.0
```

`twilio>=9.0.0` already exists.

---

## Build Order

### Phase 0: Tests Around Existing Behavior

Before refactoring:

1. Add/identify a test for `/api/data-analysis/upload` that confirms:
   - `uploaded_data.csv` exists
   - `metadata_cache.json` exists
   - `.data_analysis_mode` exists
   - `DataAnalysisStateManager` contains `column_schema`
2. Add/identify a test for TPR start through the web chat route.
3. Add/identify a test for a general flexible analysis message.

These tests protect the service extraction.

### Phase 1: Extract Upload Service

1. Create `app/services/analysis_upload_service.py`.
2. Move upload processing logic from `/api/data-analysis/upload` into the service.
3. Update the web route to call the service.
4. Verify the web UI upload still works.
5. Verify no frontend response contract changed.

### Phase 2: Extract Chat Orchestration Service

1. Create `app/services/analysis_chat_service.py`.
2. Move TPR start/active/general-agent routing into the service.
3. Update only the Data Analysis V3 sync and streaming routes in
   `app/api/data_analysis_routes.py` to call the service.
4. Verify:
   - “start the tpr workflow”
   - facility-level selection
   - age-group selection
   - flexible analysis question
   - map generation

### Phase 3: Add Public S3 Map Storage

1. Implement `app/utils/s3_map_storage.py`.
2. Add unit tests or script checks for:
   - public map upload
   - URL escaping
   - no object ACLs
   - direct S3 URL opens for a `maps/test/...` object
   - visualization resolver handles `file_path`, `path`, `/serve_viz_file/...`, and
     `visualizations/{filename}` shapes
3. Keep existing `instance_sync.py` for private session files.
4. Verify `sync_session_after_upload(session_id)` still runs after web uploads.
5. Verify `ensure_session_available(session_id)` can restore a session on the other
   EC2 instance before analysis.

### Phase 4: WhatsApp Upload

1. Add `app/whatsapp/media.py`.
2. Update `app/api/whatsapp_routes.py` media branch:
   - atomic idempotency
   - `wa_job:{MessageSid}` status handling for background failure/retry behavior
   - defensive parsing of `NumMedia`, `MessageSid`, `MediaUrl0`, and content type
   - validate media before rotating session
   - fast TwiML return
   - background job/thread calls `process_analysis_upload()`
   - `sync_session_after_upload(session_id)` after upload
3. End-to-end test with a real Twilio sandbox CSV/XLSX.

### Phase 5: WhatsApp Analysis

1. Add `app/whatsapp/responder.py`.
2. Update text branch to call `run_whatsapp_analysis_and_respond()`.
3. Add `wa_job:{MessageSid}` status handling for analysis retries.
4. Verify:
   - ask a general question after upload
   - start TPR workflow
   - continue TPR selections
   - generate map
   - tap direct S3 map link from phone
   - `wa_history` contains user and assistant turns

### Phase 6: Production Hardening

1. Add strict Redis memory behavior for WhatsApp analysis.
2. Replace daemon threads with RQ/Celery jobs, or explicitly accept thread risk for a
   short beta.
3. Add CloudWatch logs/metrics for:
   - upload received
   - media download success/failure
   - upload process success/failure
   - analysis start/end
   - Twilio send failures
   - `instance_sync` failures
   - S3 map upload failures

---

## Verification Matrix

| Scenario | Expected result |
|---|---|
| Web upload CSV | Existing UI behavior unchanged; artifacts sync through `instance_sync` |
| Web upload XLSX | `uploaded_data.csv` uses inferred header row; schema stored |
| WhatsApp upload CSV | Same artifacts/state as web upload; summary sent |
| WhatsApp upload XLSX | Same artifacts/state as web upload; summary sent |
| Twilio duplicate upload webhook while processing | Existing job is reused/ignored; no second upload session |
| Twilio retry after failed upload job | Retryable failed job can run again |
| Twilio duplicate text webhook while processing | Existing job is reused/ignored; no second analysis job |
| Twilio retry after failed analysis job | Retryable failed job can run again |
| Text hits EC2 instance without local files | `ensure_session_available()` restores session and analysis runs |
| TPR start over WhatsApp | Same first TPR prompt as web service |
| TPR selection over WhatsApp | Workflow advances, not generic LLM response |
| General question over WhatsApp | Flexible agent response sent in chunks |
| Map generated | HTML uploaded under `maps/{session_id}/`; direct S3 link opens |
| Duplicate map filename | S3 public map upload overwrites/replaces the map object safely |
| Redis memory unavailable in production | Failure is loud, not silent local fallback |
| Malformed `NumMedia` or missing `MessageSid` | Request returns 200 without crashing and logs warning |

---

## Known Non-Goals for v1

- Shapefile upload over WhatsApp.
- PDF upload over WhatsApp.
- Per-user login for WhatsApp users.
- PNG rendering of maps.
- CloudFront-backed S3 map links.
- Large-file multipart upload through WhatsApp.

---

## Implementation Notes and Pitfalls

1. Do not duplicate TPR orchestration in WhatsApp.
2. Do not call `DataAnalysisAgent` directly from WhatsApp except through the shared
   analysis chat service.
3. Do not update only `UploadService.save_file()` and assume web upload is covered;
   the active data-analysis route manually saves files today.
4. Do not rely on `get()` + `setex()` for idempotency.
5. Do not introduce private S3 session sync in v1 unless the project explicitly
   chooses a storage migration.
6. Do not rotate a WhatsApp sender to a new session before validating the media.
7. Do not use S3 object ACLs.
8. Do not build public URLs without escaping key path segments.
9. Do not rely on transitive `requests`; list it explicitly.
10. Do not silently accept local file memory on two EC2 instances for WhatsApp.
11. Do not refactor the general `/send_message` routes during this phase; extract
    Data Analysis V3 orchestration first.
12. Do not assume every visualization lives under `visualizations/{filename}`; resolve
    `file_path`, `path`, and `/serve_viz_file/...` forms.
13. Do not let an idempotency key suppress all retries after a background task fails.

---

## Final Shape

After this refactor:

- Web upload and WhatsApp upload share one upload service.
- Web chat and WhatsApp chat share one analysis orchestration service.
- Existing `instance_sync.py` handles private cross-instance session files.
- S3 `maps/*` handles public interactive map links.
- Twilio webhook remains a fast transport adapter.

This is the version that should go to implementation.
