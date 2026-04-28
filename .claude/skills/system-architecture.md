---
name: system-architecture
description: ChatMRPT system architecture and code patterns
---

# ChatMRPT System Architecture

## Core Patterns

### Flask App Factory
- Entry: `run.py` → `app/__init__.py:create_app()`
- Blueprints for route organization
- Configuration via `app/config/`

### Service Container
- `app/services/container.py` - Dependency injection
- Singleton services: `llm_manager`, `data_service`, `analysis_service`
- Lazy initialization with eager loading option

### Request Interpreter
- `app/core/request_interpreter.py` - Main LLM routing
- Handles user queries → tool selection → response generation
- Uses OpenAI function calling for tool dispatch

### Tool System
- Location: `app/tools/`
- Standard interface: each tool is self-contained
- Registration via `app/tools/tool_registry.py`
- ~41 analysis/visualization tools

## Data Flow

```
User Upload → Validation → Session Storage → Analysis Pipeline → Visualization
     ↓             ↓              ↓                 ↓                ↓
  CSV/Excel    Schema check   instance/uploads/   Scoring/PCA    Maps/Charts
  Shapefile                   {session_id}/       Ranking        HTML output
```

## Session Management
- Redis for production (multi-worker)
- Filesystem fallback for local dev
- Session data: `instance/uploads/{session_id}/`

## Key Directories
```
app/
├── core/           # LLM, sessions, request handling
├── services/       # Business logic layer
├── tools/          # Analysis tools (40+)
├── data_analysis_v3/  # LangGraph workflows
├── web/            # Routes and blueprints
├── static/         # JS, CSS, built React
└── templates/      # Jinja2 templates
```

## Frontend
- React app built to `app/static/`
- Legacy vanilla JS for some features
- Tailwind CSS styling

## Multi-Worker Considerations
- No singletons for session state
- File-based state detection
- Redis for cross-worker sessions
- 6 Gunicorn workers in production
