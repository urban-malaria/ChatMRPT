# Settlement Feedback Polish Plan

Date: 2026-05-15

## Purpose

Improve the deployed settlement classification workspace based on colleague feedback from the feature showcase.

This plan intentionally excludes the Level 2/GHSL/WorldPop discussion. That topic is outside the current settlement classification workflow. The work here stays focused on the manual Shiny-style grid classifier already merged to `main`.

## Current Baseline

The current deployed workflow supports:

- `create settlement classification`
- full-state selector map
- LGA, ward, risk-ranked, map-click, and drawn-area focus modes
- multiple basemaps
- boundary, drawn-area, and grid layer controls
- grid generation and annotation persistence
- classification session list, resume, duplicate, archive
- urban threshold filtering
- CSV and GeoJSON export

Important files:

- `app/settlement/classification.py`
- `app/api/settlement_routes.py`
- `app/agent/tools/map_tools.py`
- `app/analysis/complete_tools.py`
- `tests/settlement/test_settlement_classification.py`

## Feedback To Address

1. The side panel can become crowded, and users can lose track of what to do next.
2. Search needs suggestions and forgiving matching because ward spellings may differ.
3. Grid-size guidance needs to be clearer. The default 500m is useful, but users need to know when to go smaller or larger.
4. Grid fill can hide rooftops. The map should default to outline-first styling.
5. Add `Rural` as a settlement class.
6. If a grid cell contains mixed settlement types, users need a clear regrid-smaller workflow.
7. Users need ward-level summary results, not only raw cell annotations.
8. Urban percentage should be visible when hovering, selecting, searching, or viewing risk-ranked wards.

## Product Direction

Keep this as one map workspace, not a new assessment tool.

The panel should become a guided control surface:

- Find area
- Select focus
- Set grid
- Classify cells
- Review results
- Manage layers

The map should remain flexible: users should be able to move between state, LGA, ward, risk-ranked, drawn area, and active grid without feeling trapped in a fixed ladder.

## Branch Strategy

Create a new branch from current `main` because the prior feature branch has already been merged and deployed.

Recommended branch:

```bash
git checkout main
git pull
git checkout -b feature/settlement-feedback-polish
```

Before work starts:

- Record dirty worktree state.
- Do not revert unrelated user changes.
- Run baseline focused tests if the environment is ready.

## Phase 1: Panel IA Foundation

Goal: reduce panel crowding without changing the core behavior yet.

Scope:

- Reorganize selector panel into clearer guided sections:
  - `Find Area`
  - `Select Focus`
  - `Grid Setup`
  - `Saved Classifications`
  - `Results / Exports`
  - `Layers`
- Reorganize classifier panel around:
  - selected cell and progress
  - class/notes entry
  - navigation/filtering
  - results/export
  - layers
- Use collapsible sections or lightweight tabs inside the generated HTML.
- Keep persistent summary chips for selected focus, urban threshold, current grid, and classification progress.
- Keep guidance compact: short helper text, tooltips, and estimate warnings rather than long instructional blocks.

Likely files:

- `app/settlement/classification.py`
- `tests/settlement/test_settlement_classification.py`

Acceptance criteria:

- Existing selector and classifier behavior still works.
- Users can see a clearer order of actions.
- Layers are still accessible but no longer dominate the workflow.
- Existing settlement tests pass.

## Phase 2: Labels And Map Readability

Goal: make the classifier easier to interpret over satellite imagery.

Scope:

- Add `Rural` to the default label set.
- Add `Rural` color, legend entry, filter option, validation support, and export support.
- Change default unclassified grid style to outline-first:
  - very low or no fill
  - stronger grid borders
  - high-contrast selected-cell outline
  - subtle fill only for classified cells
- Reduce default boundary fill so satellite rooftops remain visible.
- Preserve the existing opacity controls for users who want stronger overlays.
- Add compact grid-size guidance:
  - 500m remains the default starting point.
  - smaller grids are better for mixed or dense areas.
  - larger grids are better for large areas or first-pass review.

Backward compatibility:

- Old classifications without `Rural` must still load.
- Old annotations must remain valid.
- Existing exports should not break if a classification only has the previous labels.

Likely files:

- `app/settlement/classification.py`
- `tests/settlement/test_settlement_classification.py`

Acceptance criteria:

- `Rural` can be saved and exported.
- Satellite imagery remains readable under default grid styling.
- Existing classifications still load.

## Phase 3: Urban Percentage And Search

Goal: make ward selection faster and make urban context visible where users need it.

Scope:

- Surface `urban_pct` in:
  - boundary tooltips
  - search result cards
  - visible ward cards
  - selected focus summaries
  - risk-ranked labels or hover details
- Keep the existing urban threshold filter.
- Add debounced client-side fuzzy search:
  - exact and substring matches rank highest
  - token matches rank next
  - lightweight typo tolerance for close spellings
  - avoid expensive edit-distance scoring on every keystroke
- Search results should show:
  - ward/LGA name
  - LGA
  - risk rank/category when available
  - urban percentage when available

Backward compatibility:

- Sessions without `urban_pct` should show a neutral missing-data state, not errors.

Likely files:

- `app/settlement/classification.py`
- `tests/settlement/test_settlement_classification.py`

Acceptance criteria:

- Users can find wards even with minor spelling differences.
- Urban percentage is visible during hover/select/search/risk-ranked review.
- Search remains responsive.

## Phase 4: Ward-Level Results And Export

Goal: give users the actual ward-level proportions colleagues requested.

Scope:

- Add `settlement_ward_summary.csv`.
- Include one row per ward with:
  - `ward_id`
  - `ward_name`
  - `lga`
  - `state`
  - `urban_pct`
  - `cell_size_m`
  - `total_grid_cells`
  - `classified_cells`
  - `unclassified_cells`
  - `coverage_pct`
  - count by label
  - percentage by label among classified cells
  - percentage by label among all grid cells
- Include `Formal`, `Informal`, `Slum`, `Rural`, and `No Buildings/Avoid Area` where applicable.
- Add the summary file to download links.
- Add a compact `Results / Exports` panel section that makes the ward-level summary discoverable.
- Address export freshness:
  - either refresh exports automatically after saving annotations, or
  - clearly show that users must refresh exports before downloading.

Edge cases:

- no annotations yet
- partially classified wards
- missing urban percentage
- old label sets
- empty or malformed annotation files
- no-buildings cells included in both counts and percentages

Likely files:

- `app/settlement/classification.py`
- `app/api/settlement_routes.py` if response shape changes
- `tests/settlement/test_settlement_classification.py`

Acceptance criteria:

- Export produces raw annotations, classified grid GeoJSON, metadata JSON, and ward summary CSV.
- Ward summary percentages are explicit and unambiguous.
- Tests cover partial classification and no-annotation cases.

## Phase 5: Regrid Smaller Workflow

Goal: support mixed-cell situations without forcing users to abandon their work.

Scope:

- Add duplicate/regrid support with an optional new `cell_size_m`.
- UI action:
  - `Regrid smaller` from saved classification cards and/or classifier panel.
  - Prefill half the current grid size.
  - Enforce the existing minimum of 100m.
- Preserve the old classification as a separate session.
- Do not mutate old annotations.
- Regenerate from stable ward IDs where possible.
- Store drawn geometry in new metadata for future drawn-selection regrids.
- For old drawn classifications without stored geometry, fall back to selected ward IDs and show a clear limitation.

Backward compatibility:

- Old classifications without drawn geometry must still duplicate normally.
- Existing duplicate behavior should remain available.

Likely files:

- `app/settlement/classification.py`
- `app/api/settlement_routes.py`
- `tests/settlement/test_settlement_classification.py`

Acceptance criteria:

- User can create a smaller-grid copy of the same classification area.
- Original classification and annotations remain unchanged.
- Route accepts optional `cell_size_m` safely.
- Tests cover duplicate with same size and regrid with different size.

## Phase 6: Chat Guidance And Entry Points

Goal: ensure users can discover the feature from the right workflow moments.

Scope:

- Confirm post-risk-analysis completion message still includes:
  - `create settlement classification`
- Confirm post-TPR/burden workflow guidance still clearly offers settlement classification once shapefile and enriched data are ready.
- Keep the command generic. Users should not have to specify a ward name.
- Avoid adding noisy or repeated completion text.

Likely files:

- `app/analysis/complete_tools.py`
- `app/agent/tools/map_tools.py`
- possibly formatter or TPR response files if investigation shows a separate completion path

Acceptance criteria:

- Users see a clear next-step command after risk analysis.
- Users can type `create settlement classification` without a ward name.
- Existing command routing remains intact.

## Phase 7: QA, Commits, And Push

After each implementation phase:

- Note files modified.
- Run focused tests:

```bash
pytest tests/settlement/test_settlement_classification.py
```

- Run broader tests only when the phase touches shared routes or agent behavior.
- Commit the phase with a focused message.
- Push the branch.

Final QA checklist:

- Open settlement selector locally.
- Confirm full-state map loads.
- Confirm LGA, ward, risk-ranked, map-click, and drawn selections still work.
- Confirm basemap switching works.
- Confirm grid defaults do not obscure rooftops.
- Confirm `Rural` can be saved.
- Confirm ward summary export downloads and values are correct.
- Confirm regrid smaller creates a separate classification.
- Confirm old saved classifications still load.

Deployment should wait for explicit approval after the branch has been reviewed and tested.

## Main Risks

- `app/settlement/classification.py` is large and contains generated HTML, CSS, JS, service logic, and export logic. Keep phase diffs small.
- Regrid touches multiple concepts at once: metadata, selection history, grid creation, duplicate behavior, and user expectations. Keep it isolated late in the sequence.
- Export freshness can confuse users if ward summaries are stale. This must be explicit.
- Additional guidance can make the crowded panel worse. Keep text compact and contextual.
- Old classifications may not contain new metadata fields. Every new field should have a safe fallback.

