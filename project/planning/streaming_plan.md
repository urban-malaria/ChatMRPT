# Streaming Responses Plan

**Goal:** Stream LLM tokens and tool status events from the V3 agent to the frontend in real-time, eliminating the current "dead wait" where users see nothing for 10-30 seconds.

**Status:** Planning (reviewed, issues resolved)  
**Branch:** one-brain-migration  
**Date:** 2026-04-09

---

## Current Architecture (The Problem)

```
Frontend (dataAnalysisMode=true)
  → POST /api/v1/data-analysis/chat (JSON)
  → data_analysis_routes.py calls agent.analyze()
  → agent calls graph.invoke() (BLOCKING — waits for full completion)
  → Returns complete JSON response
  → Frontend renders all at once
```

**User experience:** Click send → spinner for 10-30s → wall of text appears. No indication of what's happening. Users think it's broken.

**Competitors (Julius AI, ChatGPT):** Tokens stream in real-time. Status messages like "Running code..." appear during tool execution. Users see the response forming word by word.

---

## Target Architecture

```
Frontend (always SSE)
  → POST /api/v1/data-analysis/chat/stream (SSE)
  → New streaming handler calls agent.analyze_stream()
  → agent calls graph.astream(stream_mode=["messages", "updates"])
  → Yields SSE chunks: status → tokens → tool_start → tool_end → tokens → done
  → Frontend renders tokens as they arrive
```

**User experience:** Click send → "Analyzing your data..." → tokens stream in → "Running code..." → more tokens → complete.

---

## Implementation Plan

### Phase 1: Backend — Agent Streaming Method

**File:** `app/agent/agent.py`

Add `analyze_stream()` as an async generator alongside existing `analyze()`. Keep `analyze()` intact as fallback.

```python
async def analyze_stream(self, user_query: str, workflow_context=None):
    """
    Streaming version of analyze(). Yields SSE-ready dicts:
    
    {'type': 'status', 'content': 'Loading data...'}
    {'type': 'token', 'content': 'The'}
    {'type': 'tool_start', 'tool': 'analyze_data', 'content': 'Running analysis...'}
    {'type': 'tool_end', 'tool': 'analyze_data'}
    {'type': 'token', 'content': ' top 5 wards...'}
    {'type': 'done', 'content': '', 'visualizations': [...]}
    """
```

#### Streaming API Choice: `astream(stream_mode=["messages", "updates"])`

**Why NOT `astream_events(version="v2")`:**
- `astream_events` does NOT return the final graph state — we can't recover `output_plots` from `on_tool_end` events because those contain the raw ToolMessage, not the state update
- Our graph stores visualization paths in `output_plots` via `operator.add` in state — only state updates expose this

**Why `astream(stream_mode=["messages", "updates"])`:**
- `messages` channel provides token-level streaming (same granularity as `on_chat_model_stream`)
- `updates` channel provides full state snapshots after each node completes — this is how we capture `output_plots`
- This is LangGraph's recommended approach for production streaming with state capture

```python
full_response = ""
final_state = {}

async for stream_mode, chunk in self.graph.astream(
    input_state,
    config={"recursion_limit": 25},
    stream_mode=["messages", "updates"]
):
    if stream_mode == "messages":
        # chunk is (AIMessageChunk, metadata)
        msg_chunk, metadata = chunk
        token = msg_chunk.content
        if token and metadata.get("langgraph_node") == "agent":
            full_response += token
            yield {"type": "token", "content": token}
    
    elif stream_mode == "updates":
        # chunk is {node_name: state_update_dict}
        for node_name, state_update in chunk.items():
            # Capture output_plots from tool node updates
            if "output_plots" in state_update:
                final_state.setdefault("output_plots", [])
                final_state["output_plots"].extend(state_update["output_plots"])
            
            # Detect tool start/end from node transitions
            if node_name == "tools":
                yield {"type": "tool_start", "tool": "analyze_data",
                       "content": "Running Python analysis..."}
            elif node_name == "agent" and final_state.get("output_plots"):
                # Agent node ran after tools — tools just finished
                yield {"type": "tool_end", "tool": "analyze_data"}

# After stream completes:
visualizations = self._process_visualizations(final_state.get("output_plots", []))
yield {"type": "done", "content": "", "visualizations": visualizations}
```

**Tool status messages** (human-friendly):
```python
TOOL_STATUS = {
    "analyze_data": "Running Python analysis...",
    "create_variable_map": "Generating map...",
    "run_risk_analysis": "Running risk analysis (this may take a moment)...",
    "create_vulnerability_map": "Creating vulnerability map...",
    "plan_itn_distribution": "Calculating ITN allocation...",
    "switch_tpr_combination": "Switching TPR configuration...",
}
```

**Key decisions:**
- `astream(stream_mode=["messages", "updates"])` over `astream_events` — we need both token streaming AND state capture for visualizations
- Keep `analyze()` as non-streaming fallback for tests and any sync callers
- Add `analyze_stream_stub()` for offline/test mode (yields single token chunk + done)
- Filter message chunks: only yield tokens from the "agent" node, not from tool nodes

**Stub model streaming:**
```python
async def _analyze_stream_stub(self, user_query, workflow_context=None):
    """Offline-friendly streaming pathway for tests."""
    # ... same data loading as _analyze_with_stub ...
    stub_response = f"Dataset has {len(df)} rows and {len(df.columns)} columns."
    for word in stub_response.split():
        yield {"type": "token", "content": word + " "}
    yield {"type": "done", "content": "", "visualizations": []}
```

### Phase 2: Backend — SSE Endpoint

**File:** `app/api/data_analysis_routes.py`

Add a new streaming route alongside the existing sync one.

#### Async-to-Sync Bridge Pattern

**Problem:** Flask routes are synchronous, but `analyze_stream()` is an async generator. Calling `loop.run_until_complete(agen.__anext__())` repeatedly breaks because LangGraph's internal async queues and callbacks expect a single continuous event loop coroutine.

**Solution:** Producer/consumer with `queue.Queue` bridge:

```python
import asyncio
import queue
import threading
import json

SENTINEL = object()

@data_analysis_v3_bp.route('/api/v1/data-analysis/chat/stream', methods=['POST'])
@require_auth
def data_analysis_chat_stream():
    """SSE streaming endpoint for V3 data analysis."""
    data = request.get_json() or {}
    message = data.get('message', '')
    session_id = data.get('session_id') or session.get('session_id')
    
    # Same session realignment as sync endpoint (lines 639-656)
    # Same interaction logging (lines 667-681)
    # Same TPR routing logic
    
    app = current_app._get_current_object()
    session_data = dict(session)
    
    def generate():
        q = queue.Queue()

        async def producer():
            agent = DataAnalysisAgent(session_id)
            full_response = ""
            visualizations = []
            
            async for chunk in agent.analyze_stream(message, workflow_context):
                if chunk["type"] == "token":
                    full_response += chunk["content"]
                elif chunk["type"] == "done":
                    visualizations = chunk.get("visualizations", [])
                
                q.put(f"data: {json.dumps(chunk)}\n\n")
            
            # Save BEFORE signaling done (critical pattern from chat_stream.py)
            # Push app context since we're in a background thread
            with app.app_context():
                _save_response(session_id, session_data, full_response)
            
            q.put(SENTINEL)

        def run_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(producer())
            except Exception as e:
                q.put(f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n")
                q.put(SENTINEL)
            finally:
                loop.close()
                asyncio.set_event_loop(None)  # Clean up thread's event loop state

        threading.Thread(target=run_loop, daemon=True).start()

        while True:
            item = q.get()
            if item is SENTINEL:
                break
            yield item
    
    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    response.headers['X-Accel-Buffering'] = 'no'
    return response
```

**Why this pattern works:**
- Producer thread runs the entire async generator in a single continuous event loop (no repeated `run_until_complete`)
- `queue.Queue` is thread-safe and blocks the consumer (Flask generator) until data is available
- `asyncio.set_event_loop(None)` in `finally` prevents event loop contamination when gthread reuses the thread
- `app.app_context()` pushed explicitly in the producer thread for SessionMemory persistence
- Error handling sends error event to client before sentinel

**TPR workflow handling:**
- TPR command responses are short and don't benefit from streaming
- **Recommendation:** Always stream — single code path. TPR responses just arrive faster (one or two chunks).

### Phase 3: Frontend — Unified SSE Consumer

**File:** `frontend/src/hooks/useMessageStreaming.ts`

Replace the `if (dataAnalysisMode)` JSON branch with SSE consumption, unifying both code paths.

**Current (two paths):**
```typescript
if (dataAnalysisMode) {
  // JSON: await response.json()
} else {
  // SSE: reader.read() loop
}
```

**New (single SSE path):**
```typescript
// Always use SSE
const endpoint = dataAnalysisMode
  ? '/api/v1/data-analysis/chat/stream'  // NEW streaming endpoint
  : '/send_message_streaming';

const response = await fetch(endpoint, { ... });
const reader = response.body?.getReader();
const decoder = new TextDecoder();

// CRITICAL: Carry over the buffer pattern from existing SSE consumer (lines 177-179)
// SSE frames can be split across TCP packets
let buffer = '';
let streamingContent = '';
let visualizations: any[] = [];

while (true) {
  const { done, value } = await reader.read();
  if (done) break;
  
  buffer += decoder.decode(value, { stream: true });
  const lines = buffer.split('\n');
  buffer = lines.pop() || '';  // Keep incomplete line in buffer
  
  for (const line of lines) {
    if (!line.startsWith('data: ')) continue;
    try {
      const data = JSON.parse(line.slice(6));
      
      switch (data.type) {
        case 'token':
          streamingContent += data.content;
          updateStreamingContent(assistantMessageId, streamingContent);
          break;
        
        case 'status':
        case 'tool_start':
          updateStreamingStatus(assistantMessageId, data.content);
          break;
        
        case 'tool_end':
          clearStreamingStatus(assistantMessageId);
          break;
        
        case 'done':
          visualizations = data.visualizations || [];
          break;
          
        case 'error':
          streamingContent += `\n\nError: ${data.content}`;
          updateStreamingContent(assistantMessageId, streamingContent);
          break;
        
        // Existing standard-mode chunk format (backward compat)
        default:
          if (data.content) {
            streamingContent += data.content;
            updateStreamingContent(assistantMessageId, streamingContent);
          }
          break;
      }
    } catch (e) {
      // Malformed JSON — skip this line
      console.warn('Failed to parse SSE chunk:', line);
    }
  }
}
```

**New UI elements needed:**
- Streaming status indicator (e.g., "Running Python analysis..." with animated dots)
- Simple `<span className="streaming-status">` below the streaming text
- No new component needed — conditional render in the message bubble

**Message store changes** (`frontend/src/stores/chatStore.ts`):
- Add `streamingStatus?: string` to message type
- `updateStreamingStatus(id, status)` action
- `clearStreamingStatus(id)` action

### Phase 4: Gunicorn Worker Configuration

**File:** `gunicorn_config.py`

**Current:** `worker_class = 'sync'` — each SSE connection ties up an entire worker until the stream completes. With 6 workers, only 6 concurrent streams.

**Change to:** `worker_class = 'gthread'`

```python
worker_class = 'gthread'
threads = 4  # Each worker handles 4 concurrent requests
# Nominal capacity: workers * threads = 6 * 4 = 24 concurrent requests
# Actual thread usage per SSE stream: 2 (1 gthread worker thread + 1 producer thread)
# Peak threads at full capacity: ~48 — within Python's capabilities on t3.medium
```

**Why `gthread` over `gevent`:**
- `gevent` requires monkey-patching which can break libraries (pandas, sklearn, sqlite3)
- `gthread` is native threading — no monkey-patching, no library conflicts
- Threading is sufficient since our bottleneck is I/O (waiting for OpenAI API), not CPU
- Each SSE stream holds a thread, not a worker process

**Alternative: `gevent`** (if gthread proves insufficient):
- Higher concurrency (thousands of connections)
- But requires `pip install gevent` and monkey-patching
- Risk: pandas/sklearn/sqlite may behave unexpectedly
- Only consider if we regularly see >24 concurrent analysis streams (unlikely for our user base)

**Timeout consideration:**
- Current: 300s (5 min)
- Complex analysis can take 60-90s. Streaming keeps the connection alive with continuous data.
- Keep 300s — it's fine.

### Phase 5: CloudFront/ALB SSE Compatibility

**CloudFront:**
- CloudFront buffers responses by default, which breaks SSE
- **MUST DO before testing:** Set cache behavior for `/api/v1/data-analysis/chat/stream` with `Managed-CachingDisabled` policy
- If tokens arrive in bursts instead of individually, CloudFront buffering is the issue
- `Cache-Control: no-cache` header alone may not be sufficient — the cache behavior rule is the reliable fix

**ALB:**
- AWS ALB supports SSE natively with HTTP/1.1 chunked transfer encoding
- Idle timeout: default 60s — if no data flows for 60s, ALB drops the connection
- **Keepalive mechanism for long tools:** Send SSE comments every 15 seconds during tool execution:

```python
# In analyze_stream(), during tool execution:
# SSE comments (lines starting with ':') are valid SSE format,
# ignored by EventSource spec, and prevent proxy idle timeouts.
# The producer thread sends these while the tool runs.

async def _keepalive_during_tool(self, q, tool_future):
    """Send keepalive comments while a tool executes."""
    while not tool_future.done():
        await asyncio.sleep(15)
        if not tool_future.done():
            q.put(": keepalive\n\n")
```

**Alternative keepalive approach (simpler):** Since `astream` with `stream_mode=["messages", "updates"]` emits the state update once the tool node completes, we can simply have the producer thread send periodic keepalives whenever the queue has been empty for >10 seconds. This is simpler than tracking tool futures.

**Nginx (if applicable):**
- `X-Accel-Buffering: no` header (already set) disables buffering
- `proxy_buffering off;` in nginx config if header doesn't work

---

## File Change Summary

| File | Change | Lines (est.) |
|------|--------|-------------|
| `app/agent/agent.py` | Add `analyze_stream()` + `_analyze_stream_stub()` | +100 |
| `app/api/data_analysis_routes.py` | Add `/chat/stream` SSE endpoint with thread bridge | +80 |
| `frontend/src/hooks/useMessageStreaming.ts` | Unify to SSE, add status/error handling, keep buffer | ~50 modified |
| `frontend/src/stores/chatStore.ts` | Add `streamingStatus` to message type | +10 |
| `gunicorn_config.py` | `worker_class = 'gthread'`, add `threads` | ~3 modified |
| **Total** | | ~240 new/modified lines |

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| CloudFront buffers SSE | Tokens arrive in bursts (appears broken) | `Managed-CachingDisabled` cache behavior rule — **must configure before testing** |
| Long tool execution (30s+) with no data | ALB/proxy drops connection | SSE keepalive comments (`: keepalive\n\n`) every 15s |
| Producer thread has no Flask context | SessionMemory save fails silently | Explicit `app.app_context()` push in producer |
| gthread reuses threads with stale event loop | `RuntimeError: event loop already running` | `asyncio.set_event_loop(None)` in finally block |
| Existing sync endpoint callers break | Tests, TPR handler | Keep sync `analyze()` as-is — never remove it |
| Malformed SSE frames (TCP packet splits) | Dropped tokens | Buffer pattern carried over from existing consumer |

---

## Testing Plan

1. **Smoke test (before Phase 1):** Mock SSE endpoint with `sleep(0.1)` generator to validate thread bridge, gthread, and CloudFront behavior without LangGraph complexity
2. **Unit test:** `analyze_stream()` yields correct event types for a simple query
3. **Unit test:** `analyze_stream_stub()` works without OpenAI key
4. **Integration test:** SSE endpoint returns valid `text/event-stream` with proper formatting
5. **Manual test — simple query:** "How many rows are in the data?" → tokens stream smoothly
6. **Manual test — tool use:** "What are the top 10 wards by TPR?" → status message during tool execution, then tokens resume
7. **Manual test — visualization:** "Show me a histogram of TPR" → tokens stream, visualization appears at end
8. **Manual test — error:** "Analyze the nonexistent_column" → error appears cleanly
9. **Load test:** 5 concurrent streams don't cause worker exhaustion
10. **Proxy test:** Verify streaming works through CloudFront (not buffered)
11. **Keepalive test:** Run risk analysis (~30s tool) and verify connection stays alive

---

## Implementation Order

1. **Phase 0 (smoke test):** Mock SSE endpoint + gthread config → verify infra works
2. **Phase 1:** `analyze_stream()` in agent.py — the core streaming generator
3. **Phase 2:** SSE endpoint with thread bridge in data_analysis_routes.py
4. **Phase 3:** Frontend SSE consumer — see tokens flow
5. **Phase 4:** Gunicorn gthread — production readiness
6. **Phase 5:** CloudFront/ALB cache behavior + keepalive — verify proxy compatibility

Each phase is independently testable. Phase 0-3 can be tested locally. Phase 4-5 are deployment concerns.

---

## What This Does NOT Cover (Future)

- **Streaming for standard mode:** Already works via `/send_message_streaming` — no changes needed
- **Forced retry loop:** Agent retrying failed code automatically (separate improvement, not streaming-related)
- **Proactive insights:** Agent volunteering observations (prompt engineering, not streaming-related)
- **Cancel/abort mid-stream:** Frontend already has `AbortController` — need to wire it to kill the async generator
- **Markdown rendering during stream:** Frontend already renders markdown — tokens accumulate into valid markdown naturally
