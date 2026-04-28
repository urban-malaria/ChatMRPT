"""
Secure Python Code Executor
Based on AgenticDataAnalysis tools.py execution pattern

Phase B hardening:
- Execution timeout (configurable via CHATMRPT_ANALYZE_TIMEOUT_MS)
- Restricted imports and safe builtins
- Helper utilities: top_n, ensure_numeric, suggest_columns
"""

import sys
import os
import uuid
import pickle
import logging
import time
import multiprocessing as mp
from io import StringIO
from typing import Dict, Any, Tuple, List

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import sklearn  # Import sklearn for analysis

logger = logging.getLogger(__name__)


# --- Helpers for restricted execution ---
ALLOWED_IMPORTS = (
    'pandas', 'numpy', 'plotly', 'sklearn', 'math', 'statistics',
    'json', 're', 'datetime', 'collections', 'itertools', 'functools', 'operator',
    'random', 'scipy', 'matplotlib', 'seaborn', 'geopandas'
)
BLOCKED_IMPORTS = (
    'os', 'sys', 'subprocess', 'shutil', 'socket', 'requests', 'urllib',
    'http', 'pathlib', 'importlib', 'psutil', 'resource'
)


def _restricted_import(name, globals=None, locals=None, fromlist=(), level=0):
    # Hard block dangerous modules
    if any(name == mod or name.startswith(mod + '.') for mod in BLOCKED_IMPORTS):
        raise ImportError(f"Import not allowed: {name}")
    # Allow only the approved list
    if any(name == mod or name.startswith(mod + '.') for mod in ALLOWED_IMPORTS):
        return __import__(name, globals, locals, fromlist, level)
    # Deny by default
    raise ImportError(f"Import not allowed: {name}")


def _build_safe_builtins():
    safe_names = [
        'abs', 'min', 'max', 'sum', 'len', 'range', 'enumerate', 'zip', 'map', 'filter',
        'list', 'dict', 'set', 'tuple', 'sorted', 'round', 'any', 'all', 'print',
        'isinstance', 'issubclass', 'type'
    ]
    safe_builtins = {name: getattr(__import__('builtins'), name) for name in safe_names}
    # Common exceptions
    for exc in ('Exception', 'ValueError', 'TypeError', 'KeyError', 'IndexError', 'RuntimeError'):
        safe_builtins[exc] = getattr(__import__('builtins'), exc)
    # Custom import gate
    safe_builtins['__import__'] = _restricted_import
    return safe_builtins


def _inject_helpers(exec_globals: Dict[str, Any]):
    import difflib

    def ensure_numeric(obj, cols=None, fillna=None):
        if isinstance(obj, pd.DataFrame):
            target_cols = cols or obj.columns.tolist()
            for c in target_cols:
                try:
                    obj[c] = pd.to_numeric(obj[c], errors='coerce')
                    if fillna is not None:
                        obj[c] = obj[c].fillna(fillna)
                except Exception:
                    pass
            return obj
        else:
            try:
                s = pd.to_numeric(obj, errors='coerce')
                if fillna is not None:
                    s = s.fillna(fillna)
                return s
            except Exception:
                return obj

    def top_n(df: pd.DataFrame, by, n: int = 10, ascending: bool = False):
        if not isinstance(df, pd.DataFrame):
            raise ValueError('top_n expects a DataFrame as first argument')
        # Use resolver if present
        if 'resolve_col' in exec_globals and isinstance(by, str):
            try:
                by = exec_globals['resolve_col'](by) or by
            except Exception:
                pass
        return df.sort_values(by=by, ascending=ascending).head(n)

    def suggest_columns(name_or_query: str, cols: List[str] = None, limit: int = 5) -> List[str]:
        columns = cols or exec_globals.get('df', pd.DataFrame()).columns.tolist() if isinstance(exec_globals.get('df'), pd.DataFrame) else []
        if not columns:
            return []
        name = str(name_or_query)
        # Try direct matches first (case-insensitive)
        ci_map = {c.lower(): c for c in columns}
        if name.lower() in ci_map:
            return [ci_map[name.lower()]]
        # Fuzzy suggestions
        suggestions = difflib.get_close_matches(name, columns, n=limit)
        # Also try canonical if available
        canon = exec_globals.get('canonical_columns') or {}
        inv_canon = {v: k for k, v in canon.items()}
        if name in inv_canon and inv_canon[name] in columns:
            suggestions = [inv_canon[name]] + [s for s in suggestions if s != inv_canon[name]]
        return suggestions[:limit]


    def quick_eda(df: pd.DataFrame, group_by: str = None, percentiles: List[float] = None) -> Dict[str, Any]:
        """Return a lightweight exploratory summary (prints key stats)."""
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            print("No data available for EDA.")
            return {}
        summary = {}
        print("Dataset overview:\n-----------------")
        print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(exclude=['number']).columns.tolist()
        summary['numeric_columns'] = numeric_cols
        summary['categorical_columns'] = categorical_cols
        if numeric_cols:
            desc = df[numeric_cols].describe(percentiles=percentiles or [0.25, 0.5, 0.75]).round(3)
            print("\nNumeric summary:")
            print(desc.to_string())
            summary['numeric_summary'] = desc
        if categorical_cols:
            print("\nTop categorical values:")
            cat_summary = {}
            for col in categorical_cols[:5]:
                counts = df[col].value_counts().head(5)
                cat_summary[col] = counts
                print(f"\n{col}:")
                for idx, val in counts.items():
                    print(f"  {idx}: {val}")
            summary['categorical_summary'] = cat_summary
        if group_by and group_by in df.columns:
            grouped = df.groupby(group_by).size().sort_values(ascending=False)
            print(f"\nCounts by {group_by}:")
            print(grouped.to_string())
            summary['group_counts'] = grouped
        return summary

    def run_stat_test(
        df: pd.DataFrame,
        value_col: str,
        group_col: str = None,
        test: str = 'anova'
    ) -> Dict[str, Any]:
        """Perform a quick statistical test using scipy (prints results)."""
        from scipy import stats
        if value_col not in df:
            raise ValueError(f"Column '{value_col}' not found")
        series = pd.to_numeric(df[value_col], errors='coerce').dropna()
        if series.empty:
            raise ValueError(f"Column '{value_col}' has no numeric data")
        result = {}
        if group_col and group_col in df:
            groups = []
            for _, subset in df[[group_col, value_col]].dropna().groupby(group_col):
                groups.append(pd.to_numeric(subset[value_col], errors='coerce').dropna())
            if len(groups) < 2:
                raise ValueError('Need at least two groups for the chosen test')
            if test.lower() in {'anova', 'f_oneway'}:
                stat, pval = stats.f_oneway(*groups)
                test_name = 'One-way ANOVA'
            elif test.lower() in {'kruskal', 'kruskalwallis'}:
                stat, pval = stats.kruskal(*groups)
                test_name = 'Kruskal-Wallis H-test'
            else:
                raise ValueError(f"Unsupported group test '{test}'")
            print(f"{test_name}: F/H={stat:.4f}, p-value={pval:.4g}")
            result.update({'test': test_name, 'statistic': stat, 'p_value': pval})
        else:
            if test.lower() in {'ttest', 'ttest_ind'}:
                half = len(series) // 2
                stat, pval = stats.ttest_ind(series[:half], series[half:], equal_var=False)
                test_name = 'Two-sample t-test (split halves)'
            elif test.lower() in {'shapiro'}:
                stat, pval = stats.shapiro(series)
                test_name = 'Shapiro-Wilk normality test'
            else:
                raise ValueError(f"Unsupported single-series test '{test}'")
            print(f"{test_name}: statistic={stat:.4f}, p-value={pval:.4g}")
            result.update({'test': test_name, 'statistic': stat, 'p_value': pval})
        return result

    def train_ml_model(
        df: pd.DataFrame,
        target: str,
        features: List[str] = None,
        task: str = 'classification',
        algorithm: str = 'random_forest',
        test_size: float = 0.2,
        random_state: int = 42
    ) -> Dict[str, Any]:
        """Train a simple sklearn model and print evaluation metrics."""
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler
        from sklearn.pipeline import Pipeline
        from sklearn.metrics import accuracy_score, f1_score, r2_score, mean_absolute_error
        from sklearn.linear_model import LogisticRegression, LinearRegression
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        from sklearn.cluster import KMeans

        if target not in df:
            raise ValueError(f"Target column '{target}' not found")
        if features is None:
            features = [c for c in df.columns if c != target]
        X = df[features].select_dtypes(include=['number']).fillna(0)
        if X.empty:
            raise ValueError('No numeric features available for model training')
        y = df[target]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

        if task == 'classification':
            if algorithm == 'logistic_regression':
                model = Pipeline([('scaler', StandardScaler()), ('clf', LogisticRegression(max_iter=1000))])
            else:
                model = RandomForestClassifier(random_state=random_state)
        elif task == 'regression':
            if algorithm == 'linear_regression':
                model = LinearRegression()
            else:
                model = RandomForestRegressor(random_state=random_state)
        elif task == 'clustering':
            n_clusters = int(algorithm) if algorithm.isdigit() else 3
            model = KMeans(n_clusters=n_clusters, random_state=random_state)
            clusters = model.fit_predict(X)
            df['cluster'] = clusters
            print(f"KMeans clustering complete with {model.n_clusters} clusters")
            return {'clusters': clusters, 'model': model}
        else:
            raise ValueError(f"Unsupported task '{task}'")

        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        if task == 'classification':
            acc = accuracy_score(y_test, preds)
            f1 = f1_score(y_test, preds, average='weighted', zero_division=0)
            print(f"Accuracy: {acc:.3f} | F1: {f1:.3f}")
            metrics = {'accuracy': acc, 'f1': f1}
        else:
            r2 = r2_score(y_test, preds)
            mae = mean_absolute_error(y_test, preds)
            print(f"R^2: {r2:.3f} | MAE: {mae:.3f}")
            metrics = {'r2': r2, 'mae': mae}
        return {'model': model, 'metrics': metrics}

    def kmeans_cluster(df: pd.DataFrame, features: List[str], n_clusters: int = 3, random_state: int = 42) -> pd.DataFrame:
        """Convenience wrapper for quick clustering with reusable results."""
        from sklearn.preprocessing import StandardScaler
        from sklearn.cluster import KMeans
        if not features:
            raise ValueError('Provide at least one feature for clustering')
        subset = df[features].select_dtypes(include=['number']).fillna(0)
        if subset.empty:
            raise ValueError('Selected features contain no numeric data')
        scaled = StandardScaler().fit_transform(subset)
        model = KMeans(n_clusters=n_clusters, random_state=random_state)
        labels = model.fit_predict(scaled)
        df = df.copy()
        df['cluster'] = labels
        print(f"Assigned clusters 0..{n_clusters-1} using features {features}")
        return df
    exec_globals['ensure_numeric'] = ensure_numeric
    exec_globals['top_n'] = top_n
    exec_globals['suggest_columns'] = suggest_columns
    exec_globals['quick_eda'] = quick_eda
    exec_globals['run_stat_test'] = run_stat_test
    exec_globals['train_ml_model'] = train_ml_model
    exec_globals['kmeans_cluster'] = kmeans_cluster


def _setup_plotly_capture(exec_globals: Dict[str, Any], logger_: logging.Logger):
    # Ensure plotly_figures exists
    if 'plotly_figures' not in exec_globals:
        exec_globals['plotly_figures'] = []
    _fig_store = exec_globals['plotly_figures']

    # Wrap common plotly.express creators to auto-append the returned figure
    try:
        px_module = exec_globals.get('px', px)

        def _wrap_px(func):
            def _wrapped(*args, **kwargs):
                fig = func(*args, **kwargs)
                try:
                    _fig_store.append(fig)
                except Exception as e:
                    logger_.debug(f"Could not append px figure: {e}")
                return fig
            return _wrapped

        px_creators = [
            'bar', 'line', 'scatter', 'histogram', 'box', 'area', 'pie',
            'choropleth', 'density_heatmap', 'imshow', 'treemap', 'sunburst',
            'icicle', 'funnel', 'funnel_area', 'scatter_geo', 'choropleth_mapbox',
            'scatter_mapbox', 'line_mapbox', 'density_mapbox', 'scatter_3d',
            'parallel_coordinates', 'parallel_categories'
        ]
        for _name in px_creators:
            if hasattr(px_module, _name):
                _orig = getattr(px_module, _name)
                if not getattr(_orig, '__wrapped_for_capture__', False):
                    _wrapped = _wrap_px(_orig)
                    setattr(_wrapped, '__wrapped_for_capture__', True)
                    setattr(px_module, _name, _wrapped)
    except Exception as e:
        logger_.debug(f"px auto-capture setup failed: {e}")

    # Intercept plotly.io.show to capture figures (no-op rendering)
    try:
        def _show(fig=None, *args, **kwargs):
            if fig is not None:
                try:
                    _fig_store.append(fig)
                except Exception as ie:
                    logger_.debug(f"Could not append figure in pio.show: {ie}")
            return None

        exec_globals['pio'] = pio
        exec_globals['pio'].show = _show
    except Exception as e:
        logger_.debug(f"pio.show intercept failed: {e}")


def _secure_exec_worker(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Worker that performs secure execution and returns results via a dict."""
    import sys
    print("🔧🔧🔧 [WORKER] _secure_exec_worker CALLED 🔧🔧🔧", file=sys.stderr, flush=True)
    logger.info("🔧 [WORKER] Starting _secure_exec_worker")

    try:
        # Unpack payload
        logger.info("🔧 [WORKER STEP 1] Unpacking payload")
        code = payload['code']
        current_data = payload['current_data']
        viz_dir = payload['viz_dir']
        enable_resolver = payload['enable_resolver']
        logger.info(f"🔧 [WORKER STEP 1] Code length: {len(code)}, Data keys: {list(current_data.keys())}")

        # Build exec globals with allowed modules and safe builtins
        logger.info("🔧 [WORKER STEP 2] Building exec_globals")
        exec_globals: Dict[str, Any] = {
            'pd': pd,
            'np': np,
            'px': px,
            'go': go,
            'sklearn': sklearn,
            'plotly_figures': [],
            'saved_plots': [],
            'viz_dir': viz_dir,
            'uuid': uuid,
            'pickle': pickle,
            '__builtins__': _build_safe_builtins(),
        }
        logger.info("🔧 [WORKER STEP 2] ✅ exec_globals created")

        # Inject helpers
        logger.info("🔧 [WORKER STEP 3] Injecting helpers")
        _inject_helpers(exec_globals)
        logger.info("🔧 [WORKER STEP 3] ✅ Helpers injected")

        # Add current data
        logger.info("🔧 [WORKER STEP 4] Adding current_data to exec_globals")
        exec_globals.update(current_data)
        logger.info(f"🔧 [WORKER STEP 4] ✅ exec_globals now has keys: {list(exec_globals.keys())}")

        # Optionally inject ColumnResolver helpers
        logger.info("🔧 [WORKER STEP 5] Attempting ColumnResolver injection")
        try:
            if enable_resolver and 'df' in exec_globals:
                logger.info("🔧 [WORKER STEP 5] Importing ColumnResolver")
                from app.agent.column_validator import ColumnResolver  # was core.column_resolver
                logger.info("🔧 [WORKER STEP 5] Creating ColumnResolver instance")
                resolver = ColumnResolver(exec_globals['df'])
                exec_globals['df_norm'] = resolver.df_norm
                exec_globals['resolve_col'] = resolver.resolve
                exec_globals['canonical_columns'] = resolver.canonical_map
                exec_globals['column_aliases'] = resolver.alias_map
                logger.info("🔧 [WORKER STEP 5] ✅ ColumnResolver injected successfully")
            else:
                logger.info(f"🔧 [WORKER STEP 5] Skipping resolver (enable_resolver={enable_resolver}, has df={'df' in exec_globals})")
        except Exception as e:
            # Non-fatal
            logger.warning(f"🔧 [WORKER STEP 5] ⚠️ ColumnResolver injection failed: {e}")
    except Exception as e:
        logger.error(f"🔧 [WORKER] ❌ Failed during setup: {e}", exc_info=True)
        return {'ok': False, 'error': f'Worker setup failed: {str(e)}'}

    # Capture stdout
    logger.info("🔧 [WORKER STEP 6] Setting up stdout capture")
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    try:
        # Setup Plotly capture
        logger.info("🔧 [WORKER STEP 7] Setting up Plotly capture")
        _setup_plotly_capture(exec_globals, logger)
        logger.info("🔧 [WORKER STEP 7] ✅ Plotly capture ready")

        # Monkeypatch pandas.DataFrame.to_markdown to a safe fallback when optional
        # dependency 'tabulate' is unavailable. This prevents user/LLM code that
        # calls df.to_markdown() from failing and instead falls back to a
        # readable string table.
        logger.info("🔧 [WORKER STEP 8] Patching DataFrame.to_markdown")
        try:
            import pandas as _pd
            _orig_to_markdown = getattr(_pd.DataFrame, 'to_markdown', None)
            if _orig_to_markdown is not None:
                def _safe_to_markdown(self, *args, **kwargs):
                    try:
                        return _orig_to_markdown(self, *args, **kwargs)
                    except Exception:
                        try:
                            return self.to_string(index=kwargs.get('index', False))
                        except Exception:
                            return str(self)
                _pd.DataFrame.to_markdown = _safe_to_markdown  # type: ignore[attr-defined]
                logger.info("🔧 [WORKER STEP 8] ✅ to_markdown patched")
            else:
                logger.info("🔧 [WORKER STEP 8] to_markdown not found, skipping")
        except Exception as e:
            logger.warning(f"🔧 [WORKER STEP 8] ⚠️ Patch failed: {e}")

        # Execute user code
        logger.info("🔧 [WORKER STEP 9] Executing user code")
        logger.info(f"🔧 [WORKER STEP 9] Code to execute:\n{code[:500]}")
        exec(code, exec_globals)
        logger.info("🔧 [WORKER STEP 9] ✅ Code executed successfully")

        # Save any plotly figures
        logger.info("🔧 [WORKER STEP 10] Checking for plotly figures")
        if exec_globals.get('plotly_figures'):
            logger.info(f"🔧 [WORKER STEP 10] Found {len(exec_globals['plotly_figures'])} figures to save")
            for idx, figure in enumerate(exec_globals['plotly_figures']):
                try:
                    pickle_filename = f"{viz_dir}/{uuid.uuid4()}.pickle"
                    with open(pickle_filename, 'wb') as f:
                        pickle.dump(figure, f)
                    exec_globals['saved_plots'].append(pickle_filename)
                    logger.info(f"🔧 [WORKER STEP 10] Saved figure {idx+1} to {pickle_filename}")
                except Exception as e:
                    logger.warning(f"🔧 [WORKER STEP 10] ⚠️ Failed to save figure {idx+1}: {e}")
                    continue
        else:
            logger.info("🔧 [WORKER STEP 10] No plotly figures to save")

        logger.info("🔧 [WORKER STEP 11] Preparing result")
        output = sys.stdout.getvalue()
        logger.info(f"🔧 [WORKER STEP 11] Output length: {len(output)} chars")

        skip_keys = {
            'pd', 'np', 'px', 'go', 'sklearn', 'plotly_figures', 'saved_plots',
            'viz_dir', 'uuid', 'pickle', '__builtins__', 'pio',
            # Helper functions/objects that are re-created on every run
            'ensure_numeric', 'top_n', 'suggest_columns',
            'resolve_col', 'canonical_columns', 'column_aliases', 'df_norm'
        }

        persist_vars = {}
        for key, value in exec_globals.items():
            if key in skip_keys or str(key).startswith('_'):
                continue
            try:
                pickle.dumps(value)
            except Exception:
                logger.debug(f"🔧 [WORKER STEP 11] Skipping non-picklable '{key}'")
                continue
            persist_vars[key] = value

        logger.info(f"🔧 [WORKER STEP 11] Persisting {len(persist_vars)} variables")

        result = {
            'ok': True,
            'output': output,
            'saved_plots': list(exec_globals.get('saved_plots', [])),
            'persist': persist_vars
        }
        logger.info("🔧 [WORKER STEP 12] ✅ Result prepared, returning to parent")
        return result
    except Exception as e:
        logger.error(f"🔧 [WORKER] ❌ Execution failed: {e}", exc_info=True)
        return {'ok': False, 'error': str(e)}
    finally:
        logger.info("🔧 [WORKER STEP 13] Restoring stdout")
        sys.stdout = old_stdout
        logger.info("🔧 [WORKER] ✅ Worker function complete")


def _runner(q_: mp.Queue, payload_: Dict[str, Any]):
    """
    Wrapper function for subprocess execution.
    MUST be at module level for pickling to work with multiprocessing.
    """
    try:
        # Print to stderr first - logging might not be configured in subprocess
        import sys
        print("🚀🚀🚀 [RUNNER] SUBPROCESS STARTED 🚀🚀🚀", file=sys.stderr, flush=True)
        logger.info("🚀 [RUNNER] Starting worker subprocess")

        result = _secure_exec_worker(payload_)
        print(f"🚀 [RUNNER] Worker returned, ok={result.get('ok')}", file=sys.stderr, flush=True)
        logger.info(f"🚀 [RUNNER] Worker returned result: ok={result.get('ok')}")

        try:
            q_.put(result)
            print("🚀 [RUNNER] Result placed in queue", file=sys.stderr, flush=True)
            logger.info("🚀 [RUNNER] ✅ Result placed in queue")
        except Exception as e:
            print(f"🚀 [RUNNER] QUEUE PUT FAILED: {e}", file=sys.stderr, flush=True)
            logger.error(f"🚀 [RUNNER] ❌ Failed to put result in queue: {e}")
    except Exception as e:
        print(f"🚀🚀🚀 [RUNNER] EXCEPTION: {e}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc(file=sys.stderr)
        logger.error(f"🚀 [RUNNER] ❌ Worker function raised exception: {e}", exc_info=True)
        try:
            q_.put({'ok': False, 'error': f'Worker crashed: {str(e)}'})
        except:
            pass


class SecureExecutor:
    """
    Executes Python code in a controlled environment.
    Follows AgenticDataAnalysis pattern with security enhancements.
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.persistent_vars = {}

        # Create directories for outputs
        self.viz_dir = f"instance/uploads/{session_id}/visualizations"
        os.makedirs(self.viz_dir, exist_ok=True)

    def _filter_picklable(self, data: Dict[str, Any]) -> Dict[str, Any]:
        safe: Dict[str, Any] = {}
        for k, v in (data or {}).items():
            try:
                pickle.dumps(v)
                safe[k] = v
            except Exception:
                # Skip non-picklable (e.g., resolve_col function). We'll re-inject as needed.
                continue
        return safe

    def execute(self, code: str, current_data: Dict[str, pd.DataFrame]) -> Tuple[str, Dict[str, Any]]:
        """
        Execute Python code with persistent state and data pre-loading.

        Args:
            code: Python code to execute
            current_data: Dictionary of DataFrames to make available

        Returns:
            Tuple of (output_text, state_updates)
        """
        # Log available columns for debugging but don't modify code
        if current_data and 'df' in current_data:
            df = current_data['df']
            if isinstance(df, pd.DataFrame):
                actual_columns = list(df.columns)
                logger.debug(f"Available columns: {actual_columns[:10]}...")

        # Merge persistent vars back into current environment
        base_env = dict(self.persistent_vars)
        base_env.update(current_data or {})

        # Prepare payload for worker
        payload = {
            'code': code,
            'current_data': self._filter_picklable(base_env),
            'viz_dir': self.viz_dir,
            'enable_resolver': os.getenv('CHATMRPT_ENABLE_COLUMN_RESOLVER', '1') != '0',
        }

        timeout_ms = int(os.getenv('CHATMRPT_ANALYZE_TIMEOUT_MS', '25000'))

        # Execute in a subprocess to enforce timeout
        # MUST use 'spawn' on all platforms - 'fork' fails to execute module-level functions
        ctx = mp.get_context('spawn')
        q: mp.Queue = ctx.Queue()

        logger.info(f"⚙️ [EXECUTOR] Creating subprocess (timeout={timeout_ms}ms)")
        # Use module-level _runner function (required for multiprocessing pickling)
        proc = ctx.Process(target=_runner, args=(q, payload))

        try:
            start = time.time()
            logger.info("⚙️ [EXECUTOR] Starting subprocess")
            try:
                proc.start()
                logger.info(f"⚙️ [EXECUTOR] Subprocess started, PID: {proc.pid}")
            except Exception as start_err:
                logger.error(f"⚙️ [EXECUTOR] ❌ Failed to start subprocess: {start_err}", exc_info=True)
                raise

            logger.info("⚙️ [EXECUTOR] Waiting for subprocess to complete")
            proc.join(timeout_ms / 1000.0)

            # Check exit code
            logger.info(f"⚙️ [EXECUTOR] Subprocess exitcode: {proc.exitcode}")

            if proc.is_alive():
                logger.warning(f"⚙️ [EXECUTOR] ⚠️ Process still alive after {timeout_ms}ms, terminating")
                logger.debug("⚙️ [EXECUTOR] Code snippet (timeout): %s", code[:500])
                proc.terminate()
                proc.join(1)
                logger.error(f"Analysis code timed out after {timeout_ms}ms")
                return "", {
                    'errors': [f'Timeout: analysis exceeded {timeout_ms} ms'],
                    'current_variables': dict(self.persistent_vars),
                    'executor_ms': timeout_ms,
                    'timeout_triggered': True,
                    'restrictions_enabled': True,
                }

            logger.info(f"⚙️ [EXECUTOR] Subprocess completed in {int((time.time() - start) * 1000)}ms")

            # Get result
            logger.info("⚙️ [EXECUTOR] Checking queue for result")
            if not q.empty():
                result = q.get()
                logger.info(f"⚙️ [EXECUTOR] ✅ Got result from queue: ok={result.get('ok')}")
            else:
                logger.error("⚙️ [EXECUTOR] ❌ Queue is empty - no result returned!")
                logger.debug("⚙️ [EXECUTOR] Code snippet (no result): %s", code[:500])
                result = {'ok': False, 'error': 'No result returned'}

            if not result.get('ok'):
                err = result.get('error', 'Unknown error')
                logger.error(f"Code execution error: {err}")
                return "", {
                    'errors': [str(err)],
                    'current_variables': dict(self.persistent_vars),
                    'executor_ms': int((time.time() - start) * 1000),
                    'timeout_triggered': False,
                    'restrictions_enabled': True,
                }

            # Success path
            output = result['output']
            # Update persistent vars
            for key, value in (result.get('persist') or {}).items():
                self.persistent_vars[key] = value

            # Prepare state updates
            state_updates = {
                'current_variables': dict(self.persistent_vars),
                'output_plots': result.get('saved_plots', []),
                'executor_ms': int((time.time() - start) * 1000),
                'timeout_triggered': False,
                'restrictions_enabled': True,
            }
            return output, state_updates

        except Exception as e:
            logger.error(f"Executor error: {e}")
            return "", {
                'errors': [str(e)],
                'current_variables': dict(self.persistent_vars),
                'executor_ms': int((time.time() - start) * 1000) if 'start' in locals() else 0,
                'timeout_triggered': False,
                'restrictions_enabled': True,
            }

    def reset(self):
        """Reset the executor state."""
        self.persistent_vars = {}

    def get_available_variables(self) -> List[str]:
        """Get list of available variables in the environment."""
        return list(self.persistent_vars.keys())
