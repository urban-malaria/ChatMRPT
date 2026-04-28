# Session and Conversation Management

## Overview

ChatMRPT uses a dual-ID system to manage user sessions and prevent data bleed between users:

1. **Session ID** - Backend server session, manages file uploads and analysis state
2. **Conversation ID** - Frontend identifier, ensures data isolation between browser sessions

## How It Works

### Conversation ID Flow

1. **On App Load** (`App.tsx`):
   - `storage.ensureConversationId()` generates a UUID if none exists
   - Conversation ID is added to URL: `?conversation_id=abc123-def456...`
   - Stored in `sessionStorage` (per-tab)

2. **On Every API Request**:
   - Sent as header: `X-Conversation-ID: abc123-def456...`
   - Also accepted as URL param: `?conversation_id=...`

3. **On Backend** (`app/__init__.py`):
   - Extracted from header or URL param
   - Sanitized and stored in `g.conversation_id`
   - Used to create composite session: `{base_session_id}__{conversation_id}`

4. **File Storage**:
   - Files stored in: `instance/uploads/{base_session_id}__{conversation_id}/`
   - Each conversation has isolated data

### New Chat Button Behavior

When user clicks "New Chat":
1. Backend session is cleared via `api.session.clearSession()`
2. `resetSession()` is called which:
   - Clears old conversation ID from sessionStorage
   - Generates NEW conversation ID
   - Clears localStorage (`chat-storage`)
   - Resets all session state
3. URL is updated with new conversation ID
4. User starts completely fresh

### Chat Persistence

- **Storage**: `localStorage` (persists across refresh/browser restart)
- **Key**: `chat-storage`
- **Data Saved**:
  - Last 50 messages
  - Session info (sessionId, startTime, messageCount, uploadedFiles)

## Current Implementation Status

### Working ✓

| Feature | Status | Notes |
|---------|--------|-------|
| Conversation ID in URL | ✓ | Visible for transparency |
| Data isolation on backend | ✓ | Files stored per conversation |
| Chat persists on refresh | ✓ | Uses localStorage |
| New Chat = new conversation | ✓ | Generates fresh ID, updates URL |
| X-Conversation-ID header | ✓ | Sent with all API requests |

### Known Limitations (Acceptable for Testing)

| Issue | Description | Impact | Future Fix |
|-------|-------------|--------|------------|
| Multiple tabs share conversation | All tabs use same conversation_id (localStorage) | If user opens 2 tabs and chats in both, messages mix | Could key by tab ID or use sessionStorage for conv_id |
| URL sharing confusion | Sharing URL gives recipient the conversation_id but not the messages | Messages are in sender's localStorage, not recipient's | Expected behavior, could add explanation |
| No conversation history list | Unlike ChatGPT, we don't show past conversations in sidebar | User can't switch between old conversations | Would need backend storage of conversations |

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         BROWSER                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  sessionStorage                    localStorage                  │
│  ┌─────────────────────┐          ┌─────────────────────┐       │
│  │ conversation_id:    │          │ chat-storage:       │       │
│  │ "abc123-def456..."  │          │ { messages: [...],  │       │
│  └─────────────────────┘          │   session: {...} }  │       │
│            │                      └─────────────────────┘       │
│            │                                                     │
│            ▼                                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                      URL BAR                             │    │
│  │  https://site.com/?conversation_id=abc123-def456...      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ X-Conversation-ID: abc123-def456...
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         BACKEND                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  app/__init__.py (before_request)                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ g.conversation_id = sanitize(header or url_param)        │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  api_routes.py                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ composite_session = f"{base_session}__{conversation_id}" │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  File System                                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ instance/uploads/{base_session}__{conversation_id}/      │    │
│  │   ├── uploaded_data.csv                                  │    │
│  │   ├── ward_cache.pkl                                     │    │
│  │   └── tpr_distribution_map.html                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Key Files

| File | Purpose |
|------|---------|
| `frontend/src/utils/storage.ts` | Conversation ID storage functions |
| `frontend/src/App.tsx` | Syncs conversation ID to URL on load |
| `frontend/src/stores/chatStore.ts` | Chat state persistence with Zustand |
| `frontend/src/components/Toolbar/Toolbar.tsx` | New Chat button logic |
| `app/__init__.py` | Backend conversation ID extraction (before_request) |
| `app/web/routes/api_routes.py` | Composite session ID creation |

## Testing Checklist

- [ ] Fresh visit shows conversation_id in URL
- [ ] Refresh page - chat history persists
- [ ] Click "New Chat" - URL changes to new conversation_id
- [ ] Upload file, refresh - file still associated with session
- [ ] Two different browsers - different conversation_ids, isolated data
- [ ] Login/logout - conversation persists appropriately

## Future Improvements

1. **Per-tab conversations**: Use sessionStorage for conversation_id but localStorage for messages keyed by conversation_id
2. **Conversation history sidebar**: Store conversations on backend, show list in sidebar like ChatGPT
3. **Conversation sharing**: Allow sharing with actual message history (requires backend storage)
4. **Auto-cleanup**: Periodically clean up old session folders on backend

## Related Commits

- `1f68ec9` - feat: show conversation_id in URL for data isolation visibility
- `a271622` - fix: use localStorage instead of sessionStorage to persist chat across refresh
- `92ceba7` - fix: New Chat button now generates fresh conversation_id and updates URL
