# Agent Infrastructure Access Plan

**Goal:** Give the LLM agent direct access to infrastructure (shapefiles, population data, session files) so it can perform analyses that currently require hardcoded tools — reducing the need to write bespoke tool code for every new feature.

**Status:** Planning  
**Date:** 2026-04-09

---

## The Problem

Today, any analysis that needs data beyond the uploaded CSV requires a hardcoded tool:

| What the agent needs | Current solution | Lines of code |
|---|---|---|
| Ward shapefiles for Kwara | `run_risk_analysis` tool calls `complete_tools.py` | ~1,400 |
| WorldPop population data | TPR workflow hardcoded pipeline | ~1,100 |
| Environmental variables (rainfall, NDVI) | `complete_tools.py` loads from `data/geospatial/` | embedded |
| Save a map HTML to session folder | `create_variable_map` tool wrapper | ~200 |
| Read unified_dataset.csv after analysis | `data_loader.py` priority logic | ~200 |

If someone asks "cluster the wards by malaria burden", the agent can write the clustering code — but it can't access the shapefile to plot results on a map. So we'd need another hardcoded tool.

## The Solution: Infrastructure Helper Functions

Instead of hardcoded tools, inject helper functions into the `analyze_data` sandbox that give the agent access to infrastructure. The agent writes the analysis code; the helpers provide the data and I/O.

### Proposed Helpers

```python
# 1. Load shapefile for a state
load_shapefile(state_name) -> GeoDataFrame
# Returns ward-level boundaries for the given state
# Source: data/geospatial/shapefiles/

# 2. Load population data
load_population(state_name, age_group='all') -> DataFrame
# Returns ward-level WorldPop population estimates
# Source: www/wards_with_pop.csv or similar

# 3. Load environmental variables
load_environmental(state_name) -> DataFrame
# Returns ward-level env data (rainfall, NDVI, elevation, etc.)
# Source: data/geospatial/environmental/

# 4. Save output to session
save_to_session(filename, data) -> str
# Saves a DataFrame/GeoDataFrame/HTML string to the session folder
# Returns the file path for the user to download

# 5. Load session file
load_session_file(filename) -> DataFrame
# Loads a previously saved file from the session folder
# E.g., load_session_file('unified_dataset.csv')
```

### How They Work in Practice

User: "Cluster the wards by malaria burden and show on a map"

Agent writes:
```python
from sklearn.cluster import KMeans

# Load data the agent already has
burden = df.groupby('Ward')['TPR'].mean().reset_index()

# Clustering
km = KMeans(n_clusters=4)
burden['cluster'] = km.fit_predict(burden[['TPR']])

# Load shapefile (NEW — infrastructure access)
gdf = load_shapefile('Kwara')
merged = gdf.merge(burden, left_on='WardName', right_on='Ward')

# Plot
fig = merged.plot(column='cluster', cmap='RdYlGn_r', legend=True)
print(f"Clustered {len(burden)} wards into 4 groups")
print(burden.groupby('cluster')['TPR'].agg(['mean', 'count']))
```

No new hardcoded tool needed. The agent wrote the clustering, the helper provided the shapefile.

---

## Implementation Plan

### Phase 1: Session I/O Helpers

**Simplest, highest value.** Let the agent save/load files in the session folder.

**Files to change:**
- `app/agent/executor_simple.py` — add `save_to_session()` and `load_session_file()` to `_inject_helpers()`

```python
def save_to_session(filename, data):
    """Save data to the session folder for download."""
    session_dir = f"instance/uploads/{session_id}"
    path = os.path.join(session_dir, filename)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
    elif isinstance(data, str):
        with open(path, 'w') as f:
            f.write(data)
    print(f"Saved: {filename}")
    return path

def load_session_file(filename):
    """Load a file from the session folder."""
    path = os.path.join(f"instance/uploads/{session_id}", filename)
    if path.endswith('.csv'):
        return pd.read_csv(path)
    elif path.endswith(('.xlsx', '.xls')):
        return pd.read_excel(path)
    raise FileNotFoundError(f"{filename} not found in session")
```

**Security:** Restrict filenames to alphanumeric + underscore + dots. No path traversal.

### Phase 2: Geospatial Data Helpers

**Let the agent access shapefiles and environmental data.**

**Files to change:**
- `app/agent/executor_simple.py` — add `load_shapefile()` and `load_environmental()`
- May need to refactor shapefile loading logic out of `complete_tools.py` into a shared utility

```python
def load_shapefile(state_name):
    """Load ward-level shapefile for a Nigerian state."""
    # Reuse existing shapefile loading from data/geospatial/
    # Fuzzy-match state name to actual shapefile path
    ...
    return gpd.read_file(shp_path)

def load_environmental(state_name):
    """Load environmental variables for the state's geopolitical zone."""
    # Reuse existing env data loading from complete_tools.py
    ...
    return env_df
```

**Key challenge:** The shapefile loading logic in `complete_tools.py` is tangled with the analysis logic. Need to extract the pure data-loading part into a shared module.

### Phase 3: Population Data Helper

```python
def load_population(state_name, age_group='all'):
    """Load WorldPop population estimates by ward."""
    # Source: www/wards_with_pop.csv or equivalent
    ...
    return pop_df
```

### Phase 4: Retire Hardcoded Tools (Gradual)

Once the agent can access infrastructure, these tools become redundant:

| Tool | Status | When to retire |
|---|---|---|
| `run_trend_analysis()` | **REMOVED** (this session) | Done |
| `create_variable_map` | Keep for now | After Phase 2 (agent can make maps with geopandas) |
| `create_vulnerability_map` | Keep for now | After Phase 2 |
| `run_risk_analysis` | Keep for now | After Phase 2+3 (needs shapefile + env + population) |
| `plan_itn_distribution` | Keep longer | Complex workflow with specific output format |
| `switch_tpr_combination` | Keep | TPR workflow UI concern, not analysis |

**Retirement is gradual** — keep the tools working while the agent learns to use helpers. Remove a tool only when the agent consistently produces equal or better results without it.

---

## What NOT to Give the Agent

- **Database access** — the agent should not query SQLite/Redis directly
- **File system traversal** — only session folder and known data directories
- **Network access** — no HTTP requests from the sandbox
- **Write access outside session** — no modifying shared data files

---

## Risks

| Risk | Mitigation |
|---|---|
| Path traversal via filename manipulation | Sanitize filenames: `re.sub(r'[^a-zA-Z0-9_.-]', '', filename)` |
| Large file loads crash the sandbox | Size limits on load functions (e.g., 100MB max) |
| Agent writes bad geopandas code | Keep hardcoded tools as fallback; let agent learn |
| Shapefile loading is slow | Cache loaded GeoDataFrames in memory per session |

---

## Implementation Order

1. **Phase 1: Session I/O** — `save_to_session()`, `load_session_file()` (~30 lines)
2. **Phase 2: Geospatial** — `load_shapefile()`, `load_environmental()` (~80 lines + refactor)
3. **Phase 3: Population** — `load_population()` (~30 lines)
4. **Phase 4: Gradual retirement** — remove tools one by one as agent proves capable

Phase 1 can be done immediately. Phases 2-3 require extracting data-loading logic from `complete_tools.py` into a shared module first.
