# Settlement Classification Workflow Plan

Date: 2026-05-14

## Purpose

Build a Shiny-style manual settlement classification workflow inside ChatMRPT.

This is not the archived building-footprint settlement assessment tool. The new workflow should let users generate grid cells over uploaded ward or community boundaries, inspect satellite imagery, classify each cell, add notes, persist the annotations, and export the result for validation and follow-up planning.

The workflow should work in two modes:

- Standalone mode: after the user has uploaded/enriched data and a shapefile exists for the target area.
- Post-analysis mode: after malaria risk analysis, where risk rankings can help prioritize wards for classification.

The interaction model should be map-first with progressive filtering, not a rigid LGA-then-ward ladder.

## Product Scope

### Core User Flows

1. User uploads data and shapefile through the existing ChatMRPT data workflow.
2. User asks for settlement classification, for example:
   - "Create settlement classification."
   - "Let me classify settlements in the top 10 high-risk wards."
   - "After the malaria risk analysis, open the settlement classification tool."
3. ChatMRPT generates a full-state overview map first, with an LGA/ward/risk selector panel and a satellite layer switcher.
4. User chooses a focus mode and selection target:
   - state overview only
   - by LGA
   - by ward
   - by risk-ranked wards
   - by click/draw selection on the map
5. ChatMRPT generates grid cells only after the selected area is clear.
6. User classifies each cell and adds optional notes.
7. ChatMRPT persists each annotation.
8. User exports CSV, GeoJSON, and eventually shapefile/static map outputs.

### Selection Modes

The first screen should be a selector, not a grid.

Recommended modes:

- `By LGA`
- `By Ward`
- `By Risk-ranked wards`
- `Draw/select on map`

Behavior:

- `By LGA`: show the state map, let the user choose an LGA, then optionally narrow to wards in that LGA.
- `By Ward`: show the state map and allow direct ward search/selection.
- `By Risk-ranked wards`: when risk analysis is available, auto-suggest top wards but still let the user override the selection.
- `Draw/select on map`: let the user pick visible polygons directly when they do not want to think in admin tiers.

### Classification Labels

Minimum first-version labels:

- Formal
- Informal
- Slum

Recommended optional utility label:

- No Buildings/Avoid Area

The optional utility label preserves useful behavior from the Shiny app while still satisfying the meeting requirement for Formal, Informal, and Slum.

### Notes

Each grid cell should support optional notes. Notes are important because the meeting emphasized explaining visible features, not only selecting a class.

Notes should be length-limited and escaped on display/export to avoid injection issues.

## Relevant Existing Code

### Backend

- `app/agent/agent.py`: registers specialized tools for the data-analysis agent.
- `app/agent/tools/map_tools.py`: thin LangGraph wrappers for map/planning tools; new settlement tool should follow this pattern.
- `app/agent/viz_processor.py`: converts generated HTML paths into frontend visualization objects.
- `app/api/visualization_routes.py`: serves generated HTML visualizations through `/serve_viz_file/<session_id>/...`.
- `app/api/export_routes.py`: lists and downloads export files.
- `app/api/__init__.py`: registers route blueprints.
- `app/services/data_handler.py`: reloads raw data, shapefile data, unified datasets, and vulnerability rankings from session folders.
- `app/analysis/complete_tools.py`: post-risk-analysis response can suggest settlement classification as a next step.

### Frontend

- `frontend/src/hooks/useMessageStreaming.ts`: receives streamed agent results and visualization metadata.
- `frontend/src/components/Chat/RegularMessage.tsx`: renders assistant visualizations.
- `frontend/src/components/Visualization/VisualizationContainer.tsx`: wraps visualization controls and iframe.
- `frontend/src/components/Visualization/VisualizationFrame.tsx`: embeds generated HTML in an iframe.
- `frontend/src/components/Toolbar/Toolbar.tsx`: lists downloadable files by hard-coded categories.

### Avoided Legacy Code

Do not base this feature on `app/services/settlement_loader.py` or the archived settlement visualization/validation tools. Those are building-footprint/Kano-oriented and match the old excluded concept, not the Shiny-style manual grid workflow.

## Target Architecture

### Recommended Shape

Use a backend-generated Leaflet HTML classifier displayed inside the existing chat visualization iframe, backed by first-class Flask APIs for persistence and export.

This is the best fit because ChatMRPT already supports generated interactive HTML visualizations, and the Shiny workflow is naturally map-centric.

### Why Not a Full React Page First

A React-native classifier can come later, but the first useful version should reuse the existing visualization pipeline:

- lower frontend surface area
- easier chat integration
- similar interaction model to Shiny
- faster path to a working pilot

The generated HTML must still be treated as privileged same-origin code because the iframe allows scripts and same-origin access.

## Backend Implementation Plan

### 1. Create a New Settlement Package

Create a new package:

- `app/settlement/__init__.py`
- `app/settlement/classification.py`
- `app/settlement/grid.py`
- `app/settlement/export.py`
- `app/settlement/html_renderer.py`
- optional: `app/settlement/schemas.py`

Responsibilities:

- load session data and shapefile
- detect ward identifiers and admin hierarchy
- build the overview selector map
- support LGA, ward, and risk-ranked selection modes
- generate grid cells
- render classifier HTML
- persist annotations
- generate exports

### 2. Core Tool Class

Add a backend tool class, for example:

```python
class SettlementClassificationTool(BaseTool):
    ward_names: Optional[List[str]]
    ward_ids: Optional[List[str]]
    top_n: Optional[int]
    method: str = "composite"
    cell_size_m: int = 500
    labels: Optional[List[str]]
```

Execution behavior:

1. Resolve session folder.
2. Load data through `DataHandler`.
3. Verify shapefile exists.
4. Render the overview selector map first.
5. Wait for or accept a selected LGA/ward/risk selection.
6. Resolve selected wards or polygons.
7. Generate classification record and grid.
8. Save grid GeoJSON and metadata.
9. Render interactive classifier HTML.
10. Return a `ToolExecutionResult` with:
   - `file_path`
   - `web_path`
   - `classification_id`
   - `selection_mode`
   - selected wards
   - grid count
   - download links when available

### 3. Data Requirements

Standalone mode requires:

- uploaded/enriched tabular data available in the session
- shapefile available in the session

It should not require completed malaria risk analysis.

Post-analysis mode additionally uses:

- `analysis_vulnerability_rankings.csv` for composite ranking
- `analysis_vulnerability_rankings_pca.csv` for PCA ranking when available

If PCA is requested but unavailable or skipped, the tool should explicitly fall back to composite ranking or ask the user to choose.

### 4. Ward Key Strategy

Do not rely only on display names.

Preferred stable keys:

1. `WardCode`
2. existing unique admin/geographic ID fields
3. normalized `WardName` only when unique
4. fuzzy matching only with high confidence

The ward-list API should expose:

- stable `ward_id`
- `display_name`
- raw ward name
- ward code when available
- LGA/state fields when available
- whether the name is duplicated
- ranking fields when available

The API should also expose LGA-level groupings so the UI can filter wards by LGA before generating the grid.

If a requested ward name is ambiguous, the tool should return an actionable clarification message instead of guessing.

### 5. Grid Generation

Grid behavior should follow the Shiny concept but be safer for production:

1. Repair invalid geometries where possible.
2. Require or derive CRS.
3. Reproject to a metric CRS, preferably `GeoDataFrame.estimate_utm_crs()`.
4. Generate square grid cells using `cell_size_m`.
5. Intersect/clip cells to ward boundaries.
6. Reproject output to EPSG:4326 for GeoJSON/Leaflet.
7. Assign stable IDs such as:
   - `classification_id`
   - `ward_id`
   - `grid_id`
   - `cell_index`

Guardrails:

- set a maximum ward count per classification request
- set a maximum cell count per generated map
- reject or require confirmation for very small cell sizes
- provide clear messages when the request is too large
- do not auto-generate a large grid until the user has confirmed the area selection

Initial recommended defaults:

- `cell_size_m = 500`
- max cells: define during implementation after testing Kwara-sized data
- allow future override by user request

### 6. Classification Storage

Create a per-classification folder:

```text
instance/uploads/<session_id>/settlement/<classification_id>/
  metadata.json
  grid.geojson
  annotations.json
  annotations.csv
  classifier.html
```

Annotation schema:

```json
{
  "classification_id": "...",
  "grid_id": "...",
  "ward_id": "...",
  "ward_name": "...",
  "label": "Formal",
  "notes": "...",
  "updated_at": "...",
  "updated_by_session": "..."
}
```

Use `annotations.json` as the source of truth and derive `annotations.csv` from it.

Writes should be atomic:

1. read existing annotations
2. validate update
3. write temp JSON
4. fsync where practical
5. replace existing JSON
6. regenerate CSV

This avoids corrupted files during rapid iframe autosaves.

## Map and Imagery Plan

The overview selector and the classification view should both support multiple basemaps.

Recommended basemaps:

- Esri World Imagery
- OpenStreetMap reference
- at least one lighter satellite or hybrid layer if legally and technically supported

Use Leaflet layer controls so the user can compare imagery without leaving the workflow.

If a split-screen compare view is too much for version one, keep the base map switcher first and defer split view to a later phase.

## Settlement API Plan

Add a new blueprint:

- `app/api/settlement_routes.py`

Register it in:

- `app/api/__init__.py`

### Endpoints

Recommended first-version endpoints:

```text
GET  /api/settlement/<session_id>/status
GET  /api/settlement/<session_id>/wards
POST /api/settlement/<session_id>/classifications
GET  /api/settlement/<session_id>/classifications/<classification_id>
GET  /api/settlement/<session_id>/classifications/<classification_id>/grid
GET  /api/settlement/<session_id>/classifications/<classification_id>/annotations
POST /api/settlement/<session_id>/classifications/<classification_id>/annotations
POST /api/settlement/<session_id>/classifications/<classification_id>/export
```

The explicit create endpoint matters even if the chat agent is the primary entry point. It gives the generated iframe and future frontend UI a clean API contract.

### API Security

Settlement routes should use:

- `require_auth`
- strict session ownership checks
- safe path resolution under the session folder
- classification ID validation
- grid ID validation against the generated grid
- label allowlist validation
- note length limits
- JSON escaping/encoding for any generated HTML payload

Do not copy relaxed session mismatch behavior from the existing export route.

### API Validation

Reject:

- unknown `classification_id`
- path traversal attempts
- unknown `grid_id`
- labels outside the configured allowlist
- notes above the max length
- export requests for another session's files

## Agent Integration Plan

### 1. Add Tool Wrapper

Add a wrapper in:

- `app/agent/tools/map_tools.py`

Suggested wrapper name:

```python
create_settlement_classification
```

Tool arguments:

- `thought`
- `ward_names`
- `ward_ids`
- `top_n`
- `method`
- `cell_size_m`
- `include_no_buildings`

Wrapper behavior:

1. call the backend settlement tool
2. append returned HTML `file_path` to `graph_state["output_plots"]`
3. add canonical response
4. include download links in the response message when available

### 2. Register Tool

Update:

- `app/agent/agent.py`

Add `create_settlement_classification` to the tool imports and `self.tools`.

### 3. Update Prompt

Update:

- `app/agent/prompts/system_prompt.py`

Prompt changes:

- add settlement classification to tool-selection table
- update pipeline order
- state that settlement classification is available after upload/enrichment with shapefile
- state that top-risk/prioritized ward selection is available after risk analysis
- prevent the agent from treating it as the old building-footprint assessment tool

### 4. Update Post-Analysis Message

Update:

- `app/analysis/complete_tools.py`

After risk analysis completes, the next-step options should include settlement classification, for example:

- classify settlements in high-risk wards
- create a grid for top-ranked wards
- validate formal/informal/slum settlement patterns

## Visualization Plan

### Live Classifier HTML

The generated classifier should include:

- Leaflet map
- satellite basemap
- optional OSM/light basemap
- selected ward boundary
- grid cells
- color legend
- classification progress summary
- popup or side panel for label and notes
- save status
- export button or export instructions

The live classifier should call same-origin settlement APIs with `fetch()`.

### Leaflet Dependency Strategy

Current visualization CSP allows Plotly script hosts but not common Leaflet CDN script hosts.

Choose one of these deliberately:

1. Vendor Leaflet JS/CSS locally and serve it from same origin.
2. Inline the required Leaflet assets in the generated HTML.
3. Update the `/serve_viz_file` CSP to allow the chosen Leaflet CDN.

Recommended first choice: vendor or same-origin assets to avoid weakening CSP broadly.

Tile images can use HTTPS tile providers, but script/style dependencies should be controlled.

### HTML Escaping

All injected values must be JSON-encoded or escaped:

- ward names
- notes
- labels
- classification metadata
- API URLs

The iframe runs with `allow-scripts allow-same-origin`, so XSS handling is a core requirement, not polish.

### Live vs Exported HTML

Separate the live classifier from the downloadable map.

- Live classifier: can save annotations through APIs.
- Exported HTML map: should be read-only, with embedded grid and annotations snapshot.

If a read-only export is not ready in version one, the tool should clearly export CSV/GeoJSON first and avoid pretending the live HTML is a portable artifact.

## Frontend Plan

### 1. Chat Visualization

Use the existing assistant visualization path first.

Potential required changes:

- preserve visualization object metadata through `RegularMessage`
- allow `VisualizationFrame` to respect a passed height
- use a taller default for settlement maps

The current iframe is likely functional, but the final experience will be better if settlement maps can use 700-900px height.

### 2. Download Toolbar

Update:

- `frontend/src/components/Toolbar/Toolbar.tsx`

Required changes:

- add `settlement` to `categoryLabels`
- add `settlement` to the hard-coded render order
- make sure settlement CSV/GeoJSON/ZIP/HTML files appear when returned by `/export/list/<session_id>`

### 3. Build Output

If frontend source changes are made, rebuild the Vite frontend and ensure the built assets used by Flask are updated according to the repo's current frontend deployment pattern.

## Export Plan

### First-Version Exports

Minimum:

- annotations CSV
- classified grid GeoJSON
- metadata JSON

Phase 2:

- zipped shapefile
- read-only classified HTML map
- summary report

### Export Directory

Use:

```text
instance/exports/<session_id>/settlement_export_<classification_id>/
```

Avoid basename-only download lookup for settlement exports because multiple classifications can produce files with the same names.

Preferred download URLs should include enough identity to avoid collisions, either by:

- classification-aware settlement export route, or
- export route support for safe subdirectory tokens

### Update Existing Export Route

Update:

- `app/api/export_routes.py`

Required changes:

- list settlement export files
- include category `settlement`
- support safe download lookup for settlement export folders

Do not broaden arbitrary file search in a way that weakens path safety.

## Phased Delivery Plan

### Phase 0: Confirm Final Decisions

Decisions to confirm before coding:

- labels: include `No Buildings/Avoid Area` or keep only Formal/Informal/Slum
- note requirement: optional or required
- default cell size and maximum allowed cells
- preferred top-risk selector: composite default, PCA optional
- first export set: CSV plus GeoJSON recommended

### Phase 1: Complete Minimal Workflow

Goal: one usable end-to-end classifier from chat.

Backend:

- create `app/settlement/`
- implement ward detection and ward list
- implement grid generation with CRS guardrails
- implement classification metadata storage
- implement annotation save/load
- implement CSV and GeoJSON export
- generate live classifier HTML
- add settlement API blueprint
- add agent tool wrapper
- register agent tool
- update agent prompt

Frontend:

- ensure generated HTML renders in chat
- make iframe height acceptable for the classifier
- add settlement category to toolbar if exports are listed there in Phase 1

Acceptance criteria:

- user can request classifier for named wards
- user can request classifier for top N composite-risk wards after risk analysis
- grid appears over satellite imagery
- user can classify cells and add notes
- annotations persist after reload
- CSV and GeoJSON exports are generated
- invalid labels/grid IDs are rejected

### Phase 2: Export and UX Polish

Backend:

- add zipped shapefile export
- add read-only classified map export
- improve export route collision handling
- add summary report
- add progress/statistics endpoint

Frontend:

- improve iframe height metadata handling
- improve toolbar grouping and labels
- optionally add postMessage hooks for save/export notifications

Acceptance criteria:

- exports appear reliably in toolbar
- multiple classifications in one session do not collide
- downloaded static map is read-only and portable
- user sees classification progress clearly

### Phase 3: Advanced Workflow Integration

Enhancements:

- multi-ward batch navigation
- year-specific ranking selection for multi-year risk outputs
- PCA fallback messaging
- optional React-native classifier interface
- richer validation summary for field team review
- import previous annotations
- compare classifications against risk/urban/environment layers

## Testing Plan

### Unit Tests

Add tests for:

- ward ID/name detection
- duplicate ward names and `WardCode` handling
- fuzzy-match confidence thresholds
- CRS derivation and reprojection
- invalid geometry repair
- grid clipping
- max cell guardrails
- annotation merge/idempotency
- atomic annotation writes
- CSV and GeoJSON export creation
- PCA unavailable fallback

### Route Tests

Add tests for:

- status endpoint
- ward list endpoint
- classification create endpoint
- grid endpoint
- annotation load/save endpoint
- export endpoint
- invalid session access
- session mismatch
- path traversal attempts
- invalid classification IDs
- invalid grid IDs
- invalid labels
- overlong notes

### Frontend Tests

Add tests for:

- toolbar shows `settlement` category
- visualization frame supports taller settlement map
- generated visualization still renders as an assistant message

### Security Tests

Add tests for:

- XSS in notes
- XSS in ward names
- JSON encoding in generated HTML
- unauthorized access to another session's classification
- exported filenames and paths staying inside allowed directories

### Manual End-to-End Tests

Run:

1. Upload Kwara-style data and shapefile.
2. Request settlement classification for selected wards before risk analysis.
3. Classify several cells with labels and notes.
4. Refresh/reopen visualization and confirm annotations persist.
5. Export CSV and GeoJSON.
6. Run malaria risk analysis.
7. Request settlement classification for top-risk wards.
8. Confirm selected wards match rankings.
9. Export again and confirm files do not collide.

## Main Risks

### Session Security

Generated classifier HTML runs same-origin inside the iframe. Any API exposed to it must validate session ownership, classification IDs, grid IDs, and labels.

### Geometry Scale

Small cell sizes or many wards can produce too many grid cells. The tool needs hard limits and clear messages.

### Ambiguous Ward Names

Ward names may duplicate across LGAs or differ between CSV and shapefile. The feature should use stable codes where available and ask for clarification when ambiguous.

### CSP and Leaflet Loading

Leaflet scripts may not load under current visualization CSP if pulled from CDN. Dependency strategy must be handled explicitly.

### Export Collisions

Multiple classifications in one session can produce identical filenames. Export paths should include `classification_id` or an equivalent safe namespace.

## Review Agent Changes Incorporated

The final plan incorporates the review agent's major corrections:

- added explicit classification creation API
- tightened API authorization and path safety requirements
- added generated-HTML/XSS handling
- added stable ward-key strategy
- added CRS and cell-count guardrails
- clarified PCA fallback behavior
- clarified standalone mode requirements
- identified frontend iframe metadata limitation
- updated toolbar requirements
- separated live classifier HTML from read-only export HTML
- added atomic annotation-write requirement
- expanded testing for security, geometry, route, export, and end-to-end cases

## Recommended First Build Target

Build Phase 1 as the first implementation milestone:

> From chat, generate a settlement classification map for either selected wards or top N risk-ranked wards, let the user classify grid cells with notes, persist annotations, and export CSV plus GeoJSON.

This captures the real Shiny-style workflow and the meeting requirement without waiting for shapefile ZIP export, static HTML export, or a full React-native classifier.
