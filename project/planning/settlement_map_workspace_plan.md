# Settlement Map Workspace UX Plan

Date: 2026-05-15

## Purpose

Evolve the settlement classification feature from a selector-plus-grid tool into a fluid map workspace.

The current version works, but it still feels too much like a form beside a map. The next version should feel closer to a lightweight GIS workspace: users can zoom, pan, change focus, move between state/LGA/ward/grid context, resume prior classifications, compare layers, and generate grids only when they are ready.

## Current Baseline

The current implementation supports:

- `create settlement classification`
- full-state selector map
- LGA, ward, risk-ranked, and map-click focus modes
- multiple basemaps
- grid generation
- classification labels and notes
- CSV/GeoJSON exports

Important current files:

- `app/settlement/classification.py`
- `app/api/settlement_routes.py`
- `tests/settlement/test_settlement_classification.py`
- `project/planning/settlement_classification_plan.md`

Current limitations:

- generated HTML, CSS, JavaScript, storage, grid logic, and service logic are concentrated in `classification.py`
- boundaries are removed when a grid loads, which weakens spatial context
- no classification session list/resume workflow
- no server-side grid estimate before generation
- no strong focus history or breadcrumb/chip model
- no search or viewport-aware feature list
- no rectangle/polygon draw selection
- no browser-level test coverage for the interactive map behavior

## Product Direction

The map should remain the primary object.

The side panel should be contextual and support the user's current task:

- explore the state
- focus an LGA
- focus one or more wards
- inspect risk-ranked wards
- draw/select an area
- generate a classification grid
- classify cells
- resume or switch classification sessions

The user should never feel trapped in a fixed ladder. They should be able to move among:

- `State view`
- `LGA: <name>`
- `<N> wards selected`
- `Risk-ranked wards`
- `Drawn area`
- `Grid: <classification_id>`

This should be represented as a focus stack or focus chips, not only as `State > LGA > Ward > Grid`.

## Design Principles

- Keep the user in one map workspace.
- Preserve spatial context when switching modes.
- Treat grids as overlays, not separate map worlds.
- Make it easy to go back, switch areas, or resume work.
- Estimate grid size before generating expensive or confusing outputs.
- Use server-side geometry for authoritative selections.
- Keep the first polished version restrained and operational, not decorative.
- Avoid adding more controls until the state model is clear.

## Final Architecture Direction

### Backend

Split settlement code into smaller modules before adding more workspace behavior:

- `app/settlement/classification.py`: orchestration/service API
- `app/settlement/grid.py`: grid generation and estimate logic
- `app/settlement/storage.py`: metadata, annotations, classification listing, archive/resume helpers
- `app/settlement/html_renderer.py`: generated HTML entry points
- `app/settlement/workspace_renderer.py`: workspace-specific HTML/JS rendering if separated further

The split is not cosmetic. The current inline HTML/JS is already large enough that adding search, draw tools, layer registry, resume cards, and viewport lists inside one file will become brittle.

### API

Keep existing APIs:

- `GET /api/settlement/<session_id>/status`
- `GET /api/settlement/<session_id>/wards`
- `GET /api/settlement/<session_id>/boundaries`
- `POST /api/settlement/<session_id>/classifications`
- `GET /api/settlement/<session_id>/classifications/<classification_id>/grid`
- `GET/POST /api/settlement/<session_id>/classifications/<classification_id>/annotations`
- `POST /api/settlement/<session_id>/classifications/<classification_id>/export`

Add new APIs:

- `GET /api/settlement/<session_id>/classifications`
- `POST /api/settlement/<session_id>/classifications/estimate`
- `POST /api/settlement/<session_id>/classifications/<classification_id>/archive`
- `POST /api/settlement/<session_id>/classifications/<classification_id>/duplicate`
- `POST /api/settlement/<session_id>/selection/intersect`

The estimate endpoint should accept:

- `ward_ids`
- `top_n`
- `method`
- `drawn_geojson`
- `cell_size_m`

The estimate response should return:

- selected ward count
- estimated cell count
- approximate area
- warning level
- whether grid generation is allowed
- recommended cell size if too large

### Frontend Delivery Shape

Keep the generated Leaflet HTML approach for now.

Do not switch to a full React-native workspace yet. The current chat visualization pipeline is still the fastest path, but the generated HTML should be organized more carefully and tested better.

## Phase 1: Workspace Foundation

Goal: make the current selector structurally ready for flexible navigation and resuming work.

Backend work:

- split renderer/storage/grid logic out of `classification.py`
- add classification listing endpoint
- add classification metadata summaries
- add resume/archive/duplicate support
- add grid estimate endpoint
- add server-side selected-ward resolution shared by estimate and create
- preserve current save/export behavior

Map/workspace work:

- introduce a client-side layer registry
- keep boundaries when grid loads
- treat state/LGA/ward/focus/grid as toggleable layers
- add focus chips instead of only a rigid breadcrumb
- add clear focus and return-to-workspace behavior
- show current focus, selected ward count, estimated cell count, and classification progress

Acceptance criteria:

- user opens `create settlement classification`
- user sees full state context
- user selects LGA or ward and sees estimated grid size before generation
- user generates a grid without losing boundary context
- user can return to workspace without losing the active grid
- user can list and resume existing classifications

## Phase 2: Search and Viewport-Aware Lists

Goal: make exploration feel more like a real map product.

Features:

- search LGAs and wards
- zoom/highlight search result
- side panel list of visible LGAs/wards
- visible list updates on debounced `moveend`, not continuous pan
- actions per visible feature: zoom, select, add to focus, generate estimate
- limit list size to avoid hundreds of rows

Implementation notes:

- build a lightweight client index from ward metadata
- use Leaflet bounds intersection for visible features
- use server-provided bbox/metadata when possible
- avoid rerendering full GeoJSON lists on every map movement

Acceptance criteria:

- user can search a ward/LGA and zoom directly to it
- panel updates after zoom/pan
- user can select from map or list interchangeably

## Phase 3: Classification Continuity

Goal: support changing the user's mind midway.

Features:

- active classification cards
- progress summary per classification
- resume classification
- duplicate classification
- archive classification
- return to workspace without unloading current grid
- switch from one classification to another

Classification controls:

- next unclassified cell
- previous cell
- filter cells by label
- filter unclassified cells
- progress by ward
- dirty-state warning for unsaved notes

Acceptance criteria:

- user can classify part of one ward, switch to another, and resume the first
- existing annotations remain intact
- user can distinguish active, archived, and exported classifications

## Phase 4: Draw and Spatial Selection

Goal: let users select areas visually, not only administratively.

Features:

- rectangle select first
- polygon draw later if needed
- selected area sent to backend
- backend intersects drawn geometry against original prepared ward geometries
- selected wards returned to client
- estimate shown before grid generation

Important constraint:

Do not rely on simplified display GeoJSON for authoritative spatial selection. The drawn shape should be sent to the backend and intersected against the original prepared shapefile geometry.

Acceptance criteria:

- user draws an area
- system reports selected wards and estimated cells
- user can refine before generating grid

## Phase 5: Layer and Comparison Polish

Goal: improve comparison without overwhelming the first workspace version.

V1 comparison:

- basemap switching
- risk-ranked wards overlay
- selected focus overlay
- classification grid overlay
- classified/unclassified cell overlay

V2 comparison:

- multiple classification grid overlays
- opacity sliders
- labels toggle
- side-by-side or swipe comparison if still needed

Layer model:

- keep Leaflet native layer control initially
- add an internal layer registry first
- build custom side-panel layer controls only after layer state is stable

Basemap stance:

- keep Esri satellite as primary inspection layer
- keep OSM/light reference for labels and orientation
- treat NASA Blue Marble as regional context, not ward-level inspection imagery
- remove any basemap that repeatedly disappoints at ward zoom

## Testing Plan

### Unit Tests

- grid estimate from ward IDs
- grid estimate from top N risk-ranked wards
- large selection warning behavior
- classification listing metadata
- archive/duplicate/resume metadata
- MultiPolygon handling
- invalid geometry repair
- no-CRS shapefile behavior

### Route Tests

- `GET /classifications`
- `POST /classifications/estimate`
- archive endpoint
- duplicate endpoint
- selection intersect endpoint
- unauthorized session access
- invalid classification ID
- invalid drawn geometry

### Browser/HTML Tests

Add at least one Playwright or equivalent smoke test with mocked API responses.

It should verify:

- selector loads
- search works
- layer toggle works
- estimate appears before generate
- grid loads without losing boundary context
- back-to-workspace works
- resume card works

String smoke tests are not enough for the next phase because most of the risk is in generated JavaScript behavior.

## Implementation Sequence

1. Refactor settlement modules and add classification listing.
2. Add grid estimate endpoint and use it in the selector before generation.
3. Add layer registry and stop removing boundaries when grid loads.
4. Add focus chips, clear focus, return-to-workspace, and current status.
5. Add search and debounced viewport feature list.
6. Add classification session cards and resume/archive/duplicate.
7. Add server-backed rectangle selection.
8. Add custom layer panel and comparison polish.

## Review Agent Feedback Incorporated

The review agent identified several changes that are now included in this final plan:

- session manager moved into the foundation phase
- grid estimate endpoint moved into the foundation phase
- renderer/service split made mandatory
- rigid breadcrumb replaced with focus chips/focus stack
- draw selection specified as server-side geometry intersection
- custom layer panel deferred until a layer registry exists
- aggregate state/LGA context called out as a missing need
- viewport list given performance guardrails
- classification controls expanded beyond save/export
- comparison scope split into V1 and V2
- browser-level testing added

## Recommended Next Build Target

Build Phase 1 first:

> Refactor settlement rendering/storage/grid code, add classification listing and grid estimates, preserve boundary layers when grids load, and introduce focus chips/status so users can move around without losing context.

This is the foundation for the Google Maps-like flexibility the user is asking for.
