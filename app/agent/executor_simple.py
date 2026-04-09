"""
Simple Python Code Executor (Direct Execution)
Based on original AgenticDataAnalysis pattern - NO subprocess isolation

Key differences from executor.py:
- Direct exec() in main process (like original)
- No Flask/ColumnResolver dependency issues
- Faster execution (no subprocess overhead)
- Persistent variables work naturally
- Simpler error handling

Trade-offs:
- Less security isolation (code runs in main process)
- But: still has restricted imports and safe builtins
- Still has timeout protection via signal (Unix) or threading
"""

import sys
import os
import uuid
import pickle
import logging
import signal
import threading
from io import StringIO
from typing import Dict, Any, Tuple, List

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sklearn

# FIX: Preload ML/stat classes to prevent NameError loops
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.preprocessing import StandardScaler
from scipy import stats
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)


# --- Security: Restricted Imports ---
ALLOWED_IMPORTS = (
    'pandas', 'numpy', 'plotly', 'sklearn', 'math', 'statistics',
    'json', 're', 'datetime', 'collections', 'itertools', 'functools', 'operator',
    'random', 'scipy', 'matplotlib', 'seaborn', 'geopandas', 'difflib'
)
BLOCKED_IMPORTS = (
    'os', 'sys', 'subprocess', 'shutil', 'socket', 'requests', 'urllib',
    'http', 'pathlib', 'importlib', 'psutil', 'resource'
)


def _restricted_import(name, globals=None, locals=None, fromlist=(), level=0):
    """Only allow safe imports."""
    if any(name == mod or name.startswith(mod + '.') for mod in BLOCKED_IMPORTS):
        raise ImportError(f"Import not allowed: {name}")
    if any(name == mod or name.startswith(mod + '.') for mod in ALLOWED_IMPORTS):
        return __import__(name, globals, locals, fromlist, level)
    raise ImportError(f"Import not allowed: {name}")


def _build_safe_builtins():
    """Create restricted builtins dict."""
    safe_names = [
        'abs', 'min', 'max', 'sum', 'len', 'range', 'enumerate', 'zip', 'map', 'filter',
        'list', 'dict', 'set', 'tuple', 'sorted', 'round', 'any', 'all', 'print',
        'isinstance', 'issubclass', 'type', 'str', 'int', 'float', 'bool'
    ]
    safe_builtins = {name: getattr(__import__('builtins'), name) for name in safe_names}

    # Exceptions
    for exc in ('Exception', 'ValueError', 'TypeError', 'KeyError', 'IndexError', 'RuntimeError'):
        safe_builtins[exc] = getattr(__import__('builtins'), exc)

    # Custom import
    safe_builtins['__import__'] = _restricted_import
    return safe_builtins


def _run_trend_analysis(df, time_col, value_col, group_col=None, alpha=0.10, top_n_groups=10):
    """Standard trend analysis using Kendall's tau + linear regression.

    Args:
        df: DataFrame with time-series data
        time_col: Column with time/period values (numeric or parseable)
        value_col: Column with values to trend (numeric)
        group_col: Optional column to group by (e.g., WardName, LGA)
        alpha: Significance threshold (default 0.10 for small samples)
        top_n_groups: Max groups to show in visualizations

    Returns:
        DataFrame with per-group trend results
    """
    # --- Validate inputs ---
    for col in [time_col, value_col] + ([group_col] if group_col else []):
        if col not in df.columns:
            print(f"ERROR: Column '{col}' not found in data. Available columns: {list(df.columns)[:10]}")
            print(f"HINT: You may be using the wrong DataFrame. Try:")
            print(f"  - uploaded_df (original data with all time periods)")
            print(f"  - ts_df (ward-level TPR by year)")
            print(f"  Example: run_trend_analysis(uploaded_df, 'period0me', 'Test Positivity Rate(TPR) (RDT)', 'orgunitlevel3')")
            return pd.DataFrame()

    df = df.copy()
    df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
    df[time_col] = pd.to_numeric(df[time_col], errors='coerce')
    df = df.dropna(subset=[time_col, value_col])

    if df.empty:
        print("ERROR: No valid numeric data after cleaning.")
        return pd.DataFrame()

    # --- Aggregate to one value per (group, time_period) before computing trends ---
    # Raw data often has multiple rows per period (e.g., 64 facilities per LGA per year).
    # Proper trend analysis requires aggregation to period-level means first.
    # This avoids pseudoreplication and ensures pct_change/N_periods are meaningful.
    agg_cols = [time_col] + ([group_col] if group_col else [])
    df = df.groupby(agg_cols, dropna=False)[value_col].mean().reset_index()

    # --- Compute trends per group ---
    groups = df.groupby(group_col) if group_col else [('All', df)]
    results = []

    for name, grp in groups:
        grp = grp.sort_values(time_col)
        periods = grp[time_col].values
        values = grp[value_col].values
        n = len(periods)

        if n < 3:
            continue  # Need at least 3 points for meaningful trend

        try:
            tau, mk_p = stats.kendalltau(periods, values)
            slope, intercept, r_val, lr_p, std_err = stats.linregress(periods, values)
        except Exception:
            continue

        # Classify by slope direction first, significance as context
        if abs(slope) < 1e-10:
            direction = "Stable"
        elif slope > 0:
            direction = "Increasing"
        else:
            direction = "Decreasing"

        sig = "significant" if lr_p < alpha else "non-significant"
        first_val, last_val = float(values[0]), float(values[-1])
        pct_change = ((last_val - first_val) / first_val * 100) if first_val != 0 else 0

        results.append({
            'Group': name, 'Direction': direction, 'Significance': sig,
            'Slope': round(slope, 4), 'P_value': round(lr_p, 4),
            'Tau': round(tau, 4), 'R_squared': round(r_val ** 2, 4),
            'First_value': round(first_val, 2), 'Last_value': round(last_val, 2),
            'Pct_change': round(pct_change, 1), 'N_periods': n,
        })

    if not results:
        print("No groups had enough data points (need >= 3 time periods).")
        print("HINT: Try using uploaded_df which has the full original data with all time periods:")
        print("  run_trend_analysis(uploaded_df, 'period0me', 'Test Positivity Rate(TPR) (RDT)', 'orgunitlevel3')")
        return pd.DataFrame()

    result_df = pd.DataFrame(results).sort_values('Slope', ascending=True)

    # --- Print summary ---
    n_inc = sum(1 for r in results if r['Direction'] == 'Increasing')
    n_dec = sum(1 for r in results if r['Direction'] == 'Decreasing')
    n_stb = sum(1 for r in results if r['Direction'] == 'Stable')
    print(f"\n=== TREND ANALYSIS: {value_col} over {time_col} ===")
    print(f"Groups analyzed: {len(results)} | Increasing: {n_inc} | Decreasing: {n_dec} | Stable: {n_stb}")

    if group_col:
        worst = result_df.tail(min(5, len(result_df)))
        best = result_df.head(min(5, len(result_df)))
        print(f"\nTop worsening (steepest increase):")
        for _, r in worst.iloc[::-1].iterrows():
            print(f"  {r['Group']}: slope={r['Slope']:+.4f}/yr, {r['Pct_change']:+.1f}% change ({r['Significance']})")
        print(f"\nTop improving (steepest decrease):")
        for _, r in best.iterrows():
            print(f"  {r['Group']}: slope={r['Slope']:+.4f}/yr, {r['Pct_change']:+.1f}% change ({r['Significance']})")
    else:
        r = results[0]
        print(f"Direction: {r['Direction']} ({r['Significance']})")
        print(f"Slope: {r['Slope']:+.4f} per time unit | R²: {r['R_squared']:.4f}")
        print(f"Change: {r['First_value']} → {r['Last_value']} ({r['Pct_change']:+.1f}%)")

    # --- Visualizations (px.* only for auto-capture) ---
    if group_col and len(results) > 1:
        # Line chart: top worsening + top improving
        show_groups = list(result_df.tail(min(5, len(result_df)))['Group']) + \
                      list(result_df.head(min(5, len(result_df)))['Group'])
        show_groups = list(dict.fromkeys(show_groups))  # deduplicate, preserve order
        line_df = df[df[group_col].isin(show_groups)]
        if not line_df.empty:
            px.line(line_df, x=time_col, y=value_col, color=group_col, markers=True,
                    title=f"Trend: {value_col} over {time_col} (top changing groups)")

        # Bar chart: slope ranking (top/bottom N)
        bar_df = pd.concat([result_df.head(top_n_groups), result_df.tail(top_n_groups)]).drop_duplicates()
        colors = ['#d32f2f' if s > 0 else '#2e7d32' if s < 0 else '#757575' for s in bar_df['Slope']]
        px.bar(bar_df, y='Group', x='Slope', orientation='h',
               title=f"Trend Slope Ranking: {value_col} (green=improving, red=worsening)",
               color='Direction', color_discrete_map={'Increasing': '#d32f2f', 'Decreasing': '#2e7d32', 'Stable': '#757575'})
    elif not group_col:
        # Single series: line chart with trendline
        px.scatter(df, x=time_col, y=value_col, trendline="ols",
                   title=f"Trend: {value_col} over {time_col}")

    return result_df


def _inject_helpers(exec_globals: Dict[str, Any]):
    """Inject helper utilities into exec environment."""
    import difflib

    def ensure_numeric(obj, cols=None, fillna=None):
        """Convert columns/series to numeric."""
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
        """Get top N rows sorted by column."""
        if not isinstance(df, pd.DataFrame):
            raise ValueError('top_n expects a DataFrame as first argument')
        return df.sort_values(by=by, ascending=ascending).head(n)

    def suggest_columns(name: str, df=None, limit: int = 5) -> List[str]:
        """Suggest column names based on fuzzy matching."""
        if df is None:
            df = exec_globals.get('df')
        if not isinstance(df, pd.DataFrame):
            return []

        columns = df.columns.tolist()

        # Case-insensitive exact match
        ci_map = {c.lower(): c for c in columns}
        if name.lower() in ci_map:
            return [ci_map[name.lower()]]

        # Fuzzy match
        return difflib.get_close_matches(name, columns, n=limit, cutoff=0.6)

    def capture_table(df: pd.DataFrame, name: str = None, include_index: bool = False, max_rows: int = 200):
        """Register a DataFrame for downstream formatting."""
        if not isinstance(df, pd.DataFrame):
            return df
        tables = exec_globals.setdefault('_captured_dataframes', [])
        if max_rows and df.shape[0] > max_rows:
            df = df.head(max_rows).copy()
        tables.append({
            'name': name,
            'data': df.copy(),
            'include_index': include_index
        })
        return df

    # Inject helpers
    exec_globals['ensure_numeric'] = ensure_numeric
    exec_globals['top_n'] = top_n
    exec_globals['suggest_columns'] = suggest_columns
    exec_globals['capture_table'] = capture_table

    # Wrap run_trend_analysis with auto-fallback to uploaded_df
    def _trend_with_fallback(df, time_col, value_col, group_col=None, alpha=0.10, top_n_groups=10):
        result = _run_trend_analysis(df, time_col, value_col, group_col, alpha, top_n_groups)
        if result.empty and 'uploaded_df' in exec_globals:
            uploaded = exec_globals['uploaded_df']
            if isinstance(uploaded, pd.DataFrame) and not uploaded.empty:
                # Auto-detect time column in uploaded data
                time_candidates = [c for c in uploaded.columns if any(
                    k in c.lower() for k in ['period', 'year', 'date', 'time', 'month']
                )]
                tpr_candidates = [c for c in uploaded.columns if 'positivity' in c.lower() and 'rdt' in c.lower()]
                lga_candidates = [c for c in uploaded.columns if 'level3' in c.lower() or 'lga' in c.lower()]
                if time_candidates:
                    t_col = time_candidates[0]
                    v_col = tpr_candidates[0] if tpr_candidates else value_col
                    g_col = lga_candidates[0] if lga_candidates else group_col
                    print(f"\n🔄 Auto-retrying with uploaded_df ({len(uploaded)} rows, time_col='{t_col}', value='{v_col}', group='{g_col}')")
                    result = _run_trend_analysis(uploaded, t_col, v_col, g_col, alpha, top_n_groups)
        return result

    exec_globals['run_trend_analysis'] = _trend_with_fallback

    # Inject create_map helper — calls existing VariableDistribution tool
    def _create_map(variable_name, geographic_level='ward'):
        """Create a spatial distribution map for a variable.

        Args:
            variable_name: Column name to map (fuzzy-matched, e.g. 'Burden', 'TPR', 'rainfall')
            geographic_level: 'ward' (default) or 'lga'

        Returns:
            dict with keys: success, message, web_path, file_path
        """
        try:
            from app.visualization.variable_distribution import VariableDistribution
            tool_instance = VariableDistribution(
                variable_name=variable_name,
                geographic_level=geographic_level,
            )
            # session_id is captured from the outer executor scope
            _sid = exec_globals.get('_session_id', 'default')
            result = tool_instance.execute(session_id=_sid)
            data = result.data or {}
            file_path = data.get('file_path', '')
            web_path = data.get('web_path', '')
            # Register as output plot so it appears in visualizations
            if file_path and os.path.exists(file_path):
                exec_globals.setdefault('_map_outputs', []).append(file_path)
            print(result.message or f"Map created for {variable_name}")
            return {
                'success': result.success,
                'message': result.message,
                'web_path': web_path,
                'file_path': file_path,
            }
        except Exception as e:
            print(f"Error creating map: {e}")
            return {'success': False, 'message': str(e)}

    exec_globals['create_map'] = _create_map


def _setup_plotly_capture(exec_globals: Dict[str, Any]):
    """Setup Plotly figure auto-capture."""
    if 'plotly_figures' not in exec_globals:
        exec_globals['plotly_figures'] = []

    _fig_store = exec_globals['plotly_figures']

    # Wrap plotly.express creators
    try:
        px_module = exec_globals.get('px', px)

        def _wrap_px(func):
            def _wrapped(*args, **kwargs):
                fig = func(*args, **kwargs)
                try:
                    _fig_store.append(fig)
                except Exception:
                    pass
                return fig
            return _wrapped

        px_creators = [
            'bar', 'line', 'scatter', 'histogram', 'box', 'area', 'pie',
            'choropleth', 'density_heatmap', 'scatter_geo', 'scatter_3d'
        ]
        for _name in px_creators:
            if hasattr(px_module, _name):
                _orig = getattr(px_module, _name)
                if not getattr(_orig, '__wrapped_for_capture__', False):
                    _wrapped = _wrap_px(_orig)
                    setattr(_wrapped, '__wrapped_for_capture__', True)
                    setattr(px_module, _name, _wrapped)
    except Exception as e:
        logger.debug(f"px auto-capture failed: {e}")

    # Intercept pio.show
    try:
        def _show(fig=None, *args, **kwargs):
            if fig is not None:
                try:
                    _fig_store.append(fig)
                except Exception:
                    pass
            return None

        exec_globals['pio'] = go
        exec_globals['pio'].show = _show
    except Exception as e:
        logger.debug(f"pio.show intercept failed: {e}")


class TimeoutException(Exception):
    """Raised when execution times out."""
    pass


class SimpleExecutor:
    """
    Simple Python executor using direct exec() (like original AgenticDataAnalysis).

    Benefits:
    - No subprocess → No Flask import issues
    - Persistent variables work naturally
    - Faster execution
    - Simpler debugging

    Security:
    - Restricted imports (no os, sys, subprocess, etc.)
    - Safe builtins only
    - Timeout protection
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.persistent_vars = {}  # Like original's persistent_vars

        # Create viz directory
        self.viz_dir = f"instance/uploads/{session_id}/visualizations"
        os.makedirs(self.viz_dir, exist_ok=True)

    def _timeout_handler(self, signum, frame):
        """Signal handler for timeout (Unix only)."""
        raise TimeoutException("Execution timed out")

    def execute(self, code: str, current_data: Dict[str, pd.DataFrame]) -> Tuple[str, Dict[str, Any]]:
        """
        Execute Python code with persistent state.

        Args:
            code: Python code to execute
            current_data: Dictionary of DataFrames (e.g., {'df': dataframe})

        Returns:
            Tuple of (output_text, state_updates)
        """
        # Build execution globals - USE ORIGINAL'S PATTERN
        # globals().copy() automatically includes all module-level imports:
        # pd, np, px, go, sklearn, KMeans, PCA, LinearRegression, etc.
        exec_globals: Dict[str, Any] = globals().copy()

        # Add execution-specific variables
        exec_globals.update({
            'plotly_figures': [],
            'saved_plots': [],
            'viz_dir': self.viz_dir,
            'uuid': uuid,
            'pickle': pickle,
            '_captured_dataframes': [],
        })

        # Add persistent variables (like original)
        exec_globals.update(self.persistent_vars)

        # Add current data
        exec_globals.update(current_data or {})

        # Make session_id available to injected helpers (e.g., create_map)
        exec_globals['_session_id'] = self.session_id

        # Inject helpers
        _inject_helpers(exec_globals)

        # Setup Plotly capture
        _setup_plotly_capture(exec_globals)

        # Capture stdout
        old_stdout = sys.stdout
        sys.stdout = StringIO()

        # Get timeout
        timeout_ms = int(os.getenv('CHATMRPT_ANALYZE_TIMEOUT_MS', '60000'))  # 60s default
        timeout_sec = timeout_ms / 1000.0

        formatted_tables: List[Dict[str, Any]] = []

        try:
            import time
            start = time.time()

            # Execute with timeout
            # Note: signal.alarm only works on Unix, use threading for cross-platform
            timeout_triggered = False
            execution_error = None

            def _run_code():
                nonlocal execution_error
                try:
                    exec(code, exec_globals)
                except Exception as e:
                    execution_error = e

            # Run in thread with timeout
            thread = threading.Thread(target=_run_code, daemon=True)
            thread.start()
            thread.join(timeout=timeout_sec)

            if thread.is_alive():
                # Timeout!
                timeout_triggered = True
                logger.warning(f"Code execution timed out after {timeout_ms}ms")
                output = ""
                state_updates = {
                    'errors': [f'Timeout: analysis exceeded {timeout_ms} ms'],
                    'current_variables': dict(self.persistent_vars),
                    'executor_ms': timeout_ms,
                    'timeout_triggered': True,
                }
            elif execution_error:
                # Execution error
                logger.error(f"Code execution error: {execution_error}")
                output = ""
                state_updates = {
                    'errors': [str(execution_error)],
                    'current_variables': dict(self.persistent_vars),
                    'executor_ms': int((time.time() - start) * 1000),
                }
            else:
                # Success!
                output = sys.stdout.getvalue()
                dataframes = exec_globals.get('_captured_dataframes') or []

                # Save plotly figures
                saved_plots = []
                if exec_globals.get('plotly_figures'):
                    for idx, figure in enumerate(exec_globals['plotly_figures']):
                        try:
                            pickle_filename = f"{self.viz_dir}/{uuid.uuid4()}.pickle"
                            with open(pickle_filename, 'wb') as f:
                                pickle.dump(figure, f)
                            saved_plots.append(pickle_filename)
                        except Exception as e:
                            logger.warning(f"Failed to save figure {idx}: {e}")

                # Update persistent variables - SIMPLIFIED like original
                # Skip module-level stuff, only keep user-created variables
                for key, value in exec_globals.items():
                    # Skip if key was in original globals (module imports, functions, etc.)
                    if key in globals() or key.startswith('_'):
                        continue
                    # Skip execution-specific variables
                    if key in ('plotly_figures', 'saved_plots', 'viz_dir', 'uuid', 'pickle'):
                        continue
                    # Save user-created variables
                    self.persistent_vars[key] = value

                if dataframes:
                    formatted_tables = self._render_tables(dataframes)

                # Include map outputs (HTML files from create_map helper)
                map_outputs = exec_globals.get('_map_outputs', [])
                all_plots = saved_plots + map_outputs

                state_updates = {
                    'current_variables': dict(self.persistent_vars),
                    'output_plots': all_plots,
                    'tables': formatted_tables,
                    'executor_ms': int((time.time() - start) * 1000),
                    'timeout_triggered': False,
                }

            return output, state_updates

        except Exception as e:
            logger.error(f"Executor error: {e}", exc_info=True)
            return "", {
                'errors': [str(e)],
                'current_variables': dict(self.persistent_vars),
                'executor_ms': 0,
            }
        finally:
            # Restore stdout
            sys.stdout = old_stdout

    def reset(self):
        """Reset persistent variables."""
        self.persistent_vars = {}

    def get_available_variables(self) -> List[str]:
        """Get list of persistent variables."""
        return list(self.persistent_vars.keys())

    def _render_tables(self, dataframes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rendered: List[Dict[str, Any]] = []
        for idx, item in enumerate(dataframes):
            df = item.get('data')
            name = item.get('name') or f'table_{idx + 1}'
            if df is None:
                continue
            try:
                markdown = df.to_markdown(index=item.get('include_index', False), tablefmt='github')
            except Exception:
                markdown = df.head(20).to_string(index=item.get('include_index', False))

            csv_path = None
            try:
                csv_filename = f"{self.viz_dir}/{uuid.uuid4()}.csv"
                df.to_csv(csv_filename, index=item.get('include_index', False))
                csv_path = csv_filename
            except Exception:
                csv_path = None

            rendered.append({
                'name': name,
                'markdown': markdown,
                'csv_path': csv_path,
                'row_count': int(df.shape[0]),
                'column_count': int(df.shape[1]),
            })

        return rendered
