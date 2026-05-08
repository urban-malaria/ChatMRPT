# WhatsApp Conversational Routing and Arena Plan

**Date:** 2026-05-08  
**Status:** Final implementation plan after review  
**Scope:** WhatsApp text routing, onboarding, optional Arena Mode, and integration
with existing Data Analysis V3 and TPR workflows.

## Problem

The current WhatsApp text path treats almost every non-media message as a data
analysis request. The webhook claims the message as an `analysis` job, enqueues
it, and the worker sends `Running analysis...` before the system knows whether
the user asked for data analysis, onboarding help, education, workflow
navigation, or Arena Mode.

This creates a poor first-run user experience:

- "who are you?" becomes an analysis job.
- "what is TPR?" can return "No data found. Please upload a dataset first."
- "hello" or typo greetings can enter analysis after a session exists.
- A user with a WhatsApp session but no uploaded dataset is incorrectly treated
  as if they may be ready for analysis.

The root cause is a missing WhatsApp-specific conversation boundary. WhatsApp is
a second frontend. It needs a conversational front door before the production
analysis engine.

## Product Principles

1. **WhatsApp must feel conversational before upload.** Users should be able to
   ask basic questions before sending a file.
2. **The production DataAnalysisAgent remains the data-analysis engine.** Do not
   move onboarding, FAQs, or WhatsApp-specific routing into the agent.
3. **Web Arena stays unchanged.** This plan must not alter browser Arena routing
   or browser data-analysis routes.
4. **WhatsApp Arena is opt-in, not default.** Arena is valuable, but too slow and
   verbose to be the default path for every WhatsApp question.
5. **Routing must be deterministic in the webhook.** No LLM calls, no Arena
   calls, and no DataAnalysisAgent calls from the Twilio webhook.
6. **Upload readiness is based on upload metadata, not session existence.** A
   WhatsApp session can exist before any data is uploaded.
7. **Twilio idempotency applies to synchronous replies too.** Every user-visible
   reply should be protected by `wa_job:{MessageSid}` claim/finish behavior.

## Current Code Touchpoints

- `app/api/whatsapp_routes.py`
  - Current text branch creates or reuses a session, claims an analysis job, and
    enqueues `process_whatsapp_analysis_job`.
- `app/whatsapp/jobs.py`
  - Current analysis worker sends `Running analysis...` before the message has
    been classified.
- `app/whatsapp/session.py`
  - Stores `wa_session:{phone}`, `wa_history:{phone}`, and `wa_upload:{phone}`.
  - `wa_upload:{phone}` should become the primary WhatsApp readiness signal.
- `app/services/analysis_chat_service.py`
  - Shared web/WhatsApp orchestration for Data Analysis V3 and TPR.
  - Temporary `_no_data_answer()` should be migrated out of this shared service.
- `app/api/analysis/chat_sync.py`
  - Web Arena behavior must remain untouched.

## Final Architecture

Add a WhatsApp-specific routing layer:

```text
Twilio webhook
  -> validate signature
  -> parse sender, MessageSid, body, media
  -> get WhatsApp session manager
  -> media branch unchanged
  -> text branch:
       claim wa_job:{MessageSid}
       derive WhatsApp routing context
       classify deterministic route
       if synchronous route: send TwiML reply and finish job
       if async route: enqueue the correct RQ job and mark queued
```

The route classifier decides whether the message is:

- a direct WhatsApp reply,
- an upload-required reply,
- a production analysis/workflow job,
- an optional WhatsApp Arena job,
- or unsupported/unknown guidance.

## Derived Routing Context

Do not persist a new state machine for this feature. Derive context at request
time.

```python
session_id = mgr.get_session_id(sender)
upload_metadata = mgr.get_upload_metadata(sender)
has_ready_upload = bool(
    upload_metadata
    and upload_metadata.get("status") == "ready"
    and upload_metadata.get("session_id")
)
```

`has_ready_upload` should be true only after a successful upload job. The upload
job should store:

```python
{
    "status": "ready",
    "session_id": session_id,
    "filename": upload_result.original_filename,
    "rows": upload_result.rows,
    "cols": upload_result.cols,
    "detected_type": upload_result.detected_type,
    "uploaded_at": time.time(),
}
```

Also add upload-in-progress metadata or a sender-level upload status key so text
sent while media is processing can return:

```text
Your file is still processing. I'll message you when it's ready.
```

Suggested metadata while processing:

```python
{
    "status": "processing",
    "message_sid": message_sid,
    "filename": filename,
    "started_at": time.time(),
}
```

Workflow activity should be derived only when a ready upload exists:

```python
workflow_active = False
if has_ready_upload:
    # instantiate DataAnalysisStateManager(session_id) and check
    # is_tpr_workflow_active()
```

## New Modules

### `app/whatsapp/routing.py`

Responsible for deterministic routing only.

Suggested enum:

```python
class WhatsAppRouteType(str, Enum):
    RESET = "reset"
    WELCOME = "welcome"
    SIDE_HELP = "side_help"
    ARENA_COMMAND = "arena_command"
    TPR_ACTIVE = "tpr_active"
    TPR_START = "tpr_start"
    DATA_QUESTION = "data_question"
    NO_DATA_EDUCATION = "no_data_education"
    UPLOAD_NEEDED = "upload_needed"
    UPLOAD_PROCESSING = "upload_processing"
    UNSUPPORTED = "unsupported"
```

Suggested dataclass:

```python
@dataclass(frozen=True)
class WhatsAppRouteDecision:
    route_type: WhatsAppRouteType
    reply: str | None = None
    analysis_message: str | None = None
    arena_prompt: str | None = None
    reason: str = ""
```

Main function:

```python
def classify_whatsapp_message(
    text: str,
    *,
    has_ready_upload: bool,
    upload_processing: bool = False,
    workflow_active: bool = False,
) -> WhatsAppRouteDecision:
    ...
```

Rules:

- Deterministic only.
- No LLM calls.
- No imports from DataAnalysisAgent.
- No imports from Arena manager.
- Same text may route differently depending on context.

### `app/whatsapp/responses.py`

Owns concise WhatsApp-specific direct replies:

- identity response
- capabilities response
- upload guidance
- TPR definition
- malaria burden definition
- risk mapping definition
- ITN planning definition
- upload required response
- upload processing response
- workflow side-help responses
- optional Arena help text

Keep responses short enough for WhatsApp and pass through `chunk_text()` if
needed.

### Optional: `app/whatsapp/arena.py`

Owns WhatsApp Arena formatting and state if Arena support is implemented in the
same phase.

Responsibilities:

- parse explicit Arena commands
- run Arena in an RQ worker, never in the webhook
- format model responses compactly for WhatsApp
- store current battle state under Redis keys such as:
  - `wa_arena:{phone}`
  - `wa_arena_battle:{battle_id}`
- handle follow-up commands:
  - `A`
  - `B`
  - `tie`
  - `cancel arena`

## Routing Behavior

### Global Commands

Handled before all other routing:

- `reset`
- `restart`
- `new chat`
- `start over`

Reset clears WhatsApp session, history, upload metadata, and Arena state.

Welcome/help commands:

- `help`
- `start`
- `hi`
- `hello`
- typo/variant greetings such as `heloo`, `hey`, `good morning`

These should return direct replies, not analysis jobs.

### No Ready Upload

If `has_ready_upload` is false:

| User Message | Route |
| --- | --- |
| `who are you?` | `NO_DATA_EDUCATION` or `WELCOME` |
| `what can you do?` | `NO_DATA_EDUCATION` |
| `what is TPR?` | `NO_DATA_EDUCATION` |
| `what data do I need?` | `NO_DATA_EDUCATION` |
| `how do I upload?` | `NO_DATA_EDUCATION` |
| `map malaria burden` | `UPLOAD_NEEDED` |
| `run risk analysis` | `UPLOAD_NEEDED` |
| `calculate TPR` | `UPLOAD_NEEDED` |
| `start TPR workflow` | `UPLOAD_NEEDED` |
| `arena: explain TPR limitations` | `ARENA_COMMAND` |
| unknown text | `UNSUPPORTED` with friendly guidance |

No data-analysis RQ job should be enqueued. No `Running analysis...` should be
sent.

### Upload Processing

If a media upload for the sender is in progress:

- Non-reset text should return `UPLOAD_PROCESSING`.
- Do not start a new analysis job.
- Do not create a new no-data session.

### Ready Upload, No Active Workflow

If `has_ready_upload` is true and TPR is not active:

| User Message | Route |
| --- | --- |
| `what is TPR?` | Direct educational response |
| `who are you?` | Direct identity/capabilities response |
| `summarize my data` | `DATA_QUESTION` |
| `map malaria burden` | `DATA_QUESTION` |
| `run malaria risk analysis` | `DATA_QUESTION` |
| `start TPR workflow` | `TPR_START` |
| `arena: compare TPR interpretation methods` | `ARENA_COMMAND` |
| unknown text | usually `DATA_QUESTION` |

Only `DATA_QUESTION` and `TPR_START` enqueue
`process_whatsapp_analysis_job`.

### Active TPR Workflow

Do not reimplement the TPR workflow. The existing shared orchestration in
`analysis_chat_service.py` owns workflow state and progression.

When workflow is active:

| User Message | Route |
| --- | --- |
| `yes` | `TPR_ACTIVE` |
| `primary` | `TPR_ACTIVE` |
| `secondary` | `TPR_ACTIVE` |
| `tertiary` | `TPR_ACTIVE` |
| `u5` | `TPR_ACTIVE` |
| `o5` | `TPR_ACTIVE` |
| `pw` | `TPR_ACTIVE` |
| `all` | `TPR_ACTIVE` |
| `back` | `TPR_ACTIVE` |
| `exit` | `TPR_ACTIVE` |
| `what is primary?` | `SIDE_HELP` |
| `what does u5 mean?` | `SIDE_HELP` |
| uncertain workflow text | `TPR_ACTIVE` |

Side-help response should answer the question and remind the user what the
current expected choices are.

## WhatsApp Arena Mode

### Decision

Keep browser Arena unchanged. Add WhatsApp Arena only as explicit opt-in.

Supported triggers:

- `arena: <question>`
- `compare models: <question>`
- `expert view: <question>`

Non-trigger broad questions should not automatically enter Arena in v1.

### Why Explicit Opt-In

Arena is valuable for broad expert questions, but it is expensive and verbose
for WhatsApp. Defaulting to Arena would make routine onboarding slower and
harder to read. Explicit opt-in gives advanced users the feature without making
every conversation heavy.

### WhatsApp Arena UX

Arena responses must be compact. Do not dump the full browser UI into WhatsApp.

Suggested response format:

```text
Arena comparison

A: <short model A answer>

B: <short model B answer>

Consensus:
<brief synthesis>

Reply A, B, or tie if one answer was more useful. Type cancel arena to stop.
```

Commands:

- `A`
- `B`
- `tie`
- `cancel arena`

Arena follow-up state should be separate from data-analysis state:

- `wa_arena:{phone}`
- TTL: 24 hours or shorter
- reset clears Arena state

### Arena Implementation Boundary

The webhook may classify `ARENA_COMMAND`, but it must not run Arena. It should
enqueue a worker job such as:

```python
process_whatsapp_arena_job(sender, message_sid, prompt)
```

The worker runs the existing Arena backend or service layer and formats the
result for WhatsApp.

## Webhook Flow

Text branch target flow:

```text
if missing MessageSid:
    log and return empty TwiML

if not claim_job(redis, MessageSid, sender, "conversation"):
    return empty TwiML

derive route context:
    session_id
    upload_metadata
    has_ready_upload
    upload_processing
    workflow_active

decision = classify_whatsapp_message(...)

if decision is synchronous:
    response.message(decision.reply)
    finish_job(..., "succeeded", session_id=session_id)
    return TwiML

if decision is DATA_QUESTION / TPR_START / TPR_ACTIVE:
    ensure session_id exists
    mark job queued
    enqueue process_whatsapp_analysis_job(...)
    return empty TwiML

if decision is ARENA_COMMAND:
    mark job queued
    enqueue process_whatsapp_arena_job(...)
    return empty TwiML
```

Job type can stay `conversation` initially, or use more specific types:

- `conversation`
- `analysis`
- `arena`

The important requirement is that every user-visible response is protected by
`claim_job()` and `finish_job()`.

## Worker Changes

### Analysis Worker

`process_whatsapp_analysis_job()` should send:

```text
Running analysis...
```

only for actual analysis or workflow jobs. It should not receive ordinary
education/help messages after the route refactor.

### Arena Worker

Add a new worker only if Arena support is implemented in this phase:

```python
def process_whatsapp_arena_job(*, sender, message_sid, prompt):
    ...
```

This worker should:

1. send a short "Comparing expert answers..." acknowledgement;
2. run Arena backend/service logic;
3. format a compact WhatsApp response;
4. store battle state for `A` / `B` / `tie`;
5. finish `wa_job`.

If Arena service APIs are not clean enough yet, implement the routing and
document Arena as the next sub-phase rather than forcing a fragile integration.

## Migration From Temporary `_no_data_answer`

The temporary `_no_data_answer()` in `analysis_chat_service.py` should not be
the final fix because it is shared by web and WhatsApp orchestration and cannot
prevent premature `Running analysis...` messages.

Migration plan:

1. Move WhatsApp educational replies into `app/whatsapp/responses.py`.
2. Cover those replies with WhatsApp routing tests.
3. Remove `_no_data_answer()` from `analysis_chat_service.py`.
4. Remove or replace tests that enshrine `_no_data_answer()` as shared behavior.
5. Keep web behavior unchanged.

If a defensive no-data response is still desired in shared analysis, it should
be generic and not become the primary WhatsApp UX.

## Testing Plan

### Unit Tests

Add tests for `app/whatsapp/routing.py`:

- no upload + `who are you?` -> direct education/capabilities
- no upload + `what is TPR?` -> no-data education
- no upload + `map malaria burden` -> upload needed
- no upload + `start TPR workflow` -> upload needed
- no upload + `arena: explain TPR limitations` -> Arena command
- ready upload + `what is TPR?` -> direct education
- ready upload + `map malaria burden` -> data question
- ready upload + unknown text -> data question
- active workflow + `primary` -> TPR active
- active workflow + `what is primary?` -> side help
- upload processing + any non-reset text -> upload processing
- typo greeting `heloo` -> welcome/capabilities

### Route Tests

Add tests around `app/api/whatsapp_routes.py`:

- synchronous direct replies call `claim_job()` and `finish_job()`
- duplicate `MessageSid` does not send duplicate reply
- pre-upload educational question does not enqueue RQ
- pre-upload upload-required request does not enqueue RQ
- ready-upload data question enqueues analysis
- Arena command enqueues Arena job
- reset clears session, upload metadata, history, and Arena state

### Worker Tests

Add tests for `app/whatsapp/jobs.py`:

- analysis job sends `Running analysis...`
- direct education/help never reaches analysis job after route refactor
- Arena job formats compact WhatsApp response
- Arena vote commands update battle state

### End-to-End Twilio Sandbox Checks

Manual verification:

1. Send `heloo` before upload.
2. Send `who are you?` before upload.
3. Send `what is TPR?` before upload.
4. Send `map malaria burden` before upload.
5. Upload CSV/XLS.
6. Send `what is TPR?` after upload.
7. Send `start the TPR workflow`.
8. Send `primary` during workflow.
9. Send `what is primary?` during workflow.
10. Send `u5`.
11. Send `map malaria burden distribution`.
12. Send `arena: explain limitations of facility TPR`.
13. Reply `A`, `B`, or `tie` to the Arena comparison.

## Acceptance Criteria

- "What is TPR?" on WhatsApp before upload returns an educational answer
  synchronously.
- "Who are you?" before upload returns an identity/capabilities answer.
- "Map malaria burden" before upload returns an upload-required answer.
- No pre-upload direct-help path sends `Running analysis...`.
- No pre-upload direct-help path enqueues `process_whatsapp_analysis_job`.
- Duplicate Twilio delivery of the same `MessageSid` sends at most one visible
  response.
- A sender with `wa_session` but no ready `wa_upload` is treated as no-data.
- A sender with ready upload metadata asking "summarize my data" routes to
  DataAnalysisAgent.
- Active TPR selections route to the existing TPR workflow.
- Active TPR side-help answers without advancing the workflow.
- `arena: ...` triggers optional WhatsApp Arena.
- The same question without the Arena prefix does not trigger Arena by default.
- Web Arena behavior is unchanged.
- Temporary `_no_data_answer()` is removed from shared analysis orchestration or
  explicitly left only as a generic defensive fallback.

## Implementation Phases

### Phase 1: Deterministic WhatsApp Routing

Files:

- `app/whatsapp/routing.py`
- `app/whatsapp/responses.py`
- tests for routing and responses

Goal:

- classify WhatsApp messages using upload readiness and workflow context.

### Phase 2: Webhook Refactor

Files:

- `app/api/whatsapp_routes.py`
- tests for route idempotency and enqueue behavior

Goal:

- claim all text `MessageSid`s;
- classify before enqueue;
- return synchronous TwiML for direct replies;
- enqueue only real analysis/workflow/Arena jobs.

### Phase 3: Worker Cleanup

Files:

- `app/whatsapp/jobs.py`

Goal:

- `Running analysis...` appears only for actual analysis/workflow jobs.

### Phase 4: Arena Opt-In

Files:

- `app/whatsapp/arena.py`
- `app/whatsapp/jobs.py`
- possibly service-layer wrapper around existing Arena backend

Goal:

- support `arena:` and `compare models:` over WhatsApp without changing browser
  Arena.

If Arena backend APIs are not clean enough, stop after route classification and
document Arena worker implementation as the next sub-phase.

### Phase 5: Remove Temporary Shared No-Data Patch

Files:

- `app/services/analysis_chat_service.py`
- tests

Goal:

- move WhatsApp onboarding out of shared analysis orchestration.

### Phase 6: Full Verification

Run:

- focused unit tests
- route tests
- worker tests
- Twilio sandbox manual checks
- deployed EC2 checks with Redis/RQ worker enabled

## Non-Goals

- Do not change browser Arena routing.
- Do not make Arena the default WhatsApp response path.
- Do not run LLMs or Arena inside the Twilio webhook.
- Do not duplicate TPR workflow state machines in WhatsApp.
- Do not treat a WhatsApp session ID as proof of uploaded data.
- Do not introduce a new persistent routing state machine unless future evidence
  shows derived context is insufficient.

## Review Summary

The review process found that the plan is feasible only if routing is
route-first and context-aware. The key corrections were:

1. Use `wa_upload:{phone}` readiness, not session presence.
2. Classify before enqueueing analysis.
3. Preserve idempotency for synchronous replies.
4. Keep WhatsApp education/help outside the shared analysis service.
5. Keep web Arena unchanged.
6. Make WhatsApp Arena explicit opt-in with compact UX and battle state.
7. Handle upload-in-progress text.
8. Delegate active TPR progression to the existing shared workflow.

This is the final implementation reference for the WhatsApp conversational
routing fix.
