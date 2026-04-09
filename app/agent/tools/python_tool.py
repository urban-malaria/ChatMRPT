"""
Python Analysis Tool
Based on AgenticDataAnalysis complete_python_task tool
"""

import os
import logging
import pandas as pd
from typing import Tuple, Dict, Any, Annotated
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState

from app.agent.executor_simple import SimpleExecutor  # Use simple executor (direct exec, like original)
from app.agent.metadata_cache import MetadataCache
from app.agent.lazy_loader import LazyDataLoader
from app.agent.encoding_handler import EncodingHandler

logger = logging.getLogger(__name__)


@tool
def analyze_data(
    graph_state: Annotated[dict, InjectedState],
    thought: str,
    python_code: str
) -> Tuple[str, Dict[str, Any]]:
    """Execute Python code for data analysis on the loaded dataset.

    Use this for any data question: rankings, statistics, comparisons,
    correlations, regressions, clustering, visualizations.

    Args:
        thought: Your reasoning about what to analyze, which columns to
                 use, and what output to expect.
        python_code: Python code to execute.

    Rules:
    - MUST use print() for all outputs — code without print produces nothing
    - Data available as: df (primary), ts_df (time series), uploaded_df (original)
    - Plotly figures: append to plotly_figures list (auto-displayed)
    - Helpers: top_n(), ensure_numeric(), suggest_columns(), capture_table(),
      run_trend_analysis(), create_map()
    - Libraries: pandas, numpy, scipy.stats, sklearn, plotly, matplotlib, geopandas
    - Timeout: 60 seconds — for large data use df.sample() or df.head()
    """
    # Get session ID from graph state
    session_id = graph_state.get('session_id', 'default')

    executor = SimpleExecutor(session_id)  # Use simple executor (no subprocess)
    
    # Load data like the original - from input_data in graph_state
    current_data = {}
    
    # Load datasets from input_data (like original AgenticDataAnalysis)
    if graph_state and "input_data" in graph_state:
        for dataset in graph_state["input_data"]:
            var_name = dataset.get("variable_name")
            
            # CRITICAL FIX: First check if data is already loaded in the state
            if "data" in dataset and dataset["data"] is not None:
                # Use the pre-loaded DataFrame from state
                current_data[var_name] = dataset["data"]
                
                # Also make available as 'df' for convenience
                if 'df' not in current_data:
                    current_data['df'] = dataset["data"]
                
                logger.info(f"Using pre-loaded {var_name} with shape {dataset['data'].shape}")
            
            # Fallback: Try to load from file path if data not in state
            elif var_name and dataset.get("data_path"):
                data_path = dataset.get("data_path")
                if os.path.exists(data_path):
                    try:
                        if data_path.endswith('.csv'):
                            current_data[var_name] = EncodingHandler.read_csv_with_encoding(data_path)
                        elif data_path.endswith(('.xlsx', '.xls')):
                            current_data[var_name] = EncodingHandler.read_excel_with_encoding(data_path)
                        
                        # Also make available as 'df' for convenience
                        if 'df' not in current_data:
                            current_data['df'] = current_data[var_name]
                            
                        logger.info(f"Loaded {var_name} from {data_path}")
                    except Exception as e:
                        logger.error(f"Error loading {var_name}: {e}")
    
    # Add any existing variables from state (like original)
    if graph_state and 'current_variables' in graph_state:
        current_data.update(graph_state['current_variables'])

    # Fallback: If no DataFrame provided via input_data, try UnifiedDataState by session_id
    if 'df' not in current_data and session_id:
        try:
            from app.services.data_state import get_data_state
            uds = get_data_state(session_id)
            if uds and uds.current_data is not None:
                current_data['df'] = uds.current_data
                logger.info(f"Loaded df from UnifiedDataState for session {session_id}: {current_data['df'].shape}")
        except Exception as e:
            logger.debug(f"UnifiedDataState fallback failed: {e}")

    # Note: Column resolver removed - not needed with simple executor
    # Agent can use pandas directly like the original AgenticDataAnalysis
    # If column name fuzzy matching is needed, agent can use suggest_columns() helper
    
    # Log the code being executed
    logger.info(f"Executing Python code for session {session_id}:")
    logger.info(f"Code length: {len(python_code)} chars")  # Don't truncate
    logger.info(f"Available data variables: {list(current_data.keys())}")
    if 'df' in current_data:
        logger.info(f"DataFrame shape: {current_data['df'].shape}")
        logger.info(f"DataFrame has {len(current_data['df'].columns)} columns")  # Don't truncate
    
    # Execute the code
    _exec_start = pd.Timestamp.utcnow()
    output, state_updates = executor.execute(python_code, current_data)
    
    # Log execution results
    logger.info(f"Execution completed for session {session_id}")
    logger.info(f"Output length: {len(output)} chars")
    # Don't truncate output in logs - it affects what LLM sees

    # Check for errors
    if state_updates.get('errors'):
        logger.error(f"Execution errors: {state_updates['errors']}")
    
    if state_updates.get('output_plots'):
        logger.info(f"Generated {len(state_updates['output_plots'])} visualizations")
    
    # CRITICAL: Update the graph state with visualizations and variables
    # This is how the agent will know about generated plots
    if graph_state is not None:
        # Update current variables in the state
        if 'current_variables' in state_updates:
            graph_state['current_variables'] = state_updates['current_variables']
        
        # Add new plots to the state's output_plots list
        if 'output_plots' in state_updates and state_updates['output_plots']:
            if 'output_plots' not in graph_state:
                graph_state['output_plots'] = []
            graph_state['output_plots'].extend(state_updates['output_plots'])
            logger.info(f"Added {len(state_updates['output_plots'])} plots to graph state")
    
    # Format output using new ResponseFormatter
    from app.agent.formatters import ResponseFormatter
    # Apply normalization to fix bullet points and spacing
    formatted_output = ResponseFormatter.normalize_spacing(output)

    # If code ran with no errors but produced no output, tell the LLM explicitly.
    # Without this, the LLM sees an empty tool result and guesses wrong (e.g.,
    # concluding "no secondary facilities" when the code simply forgot to print).
    if not formatted_output.strip() and not state_updates.get('errors'):
        formatted_output = (
            "⚠️ Code executed successfully but produced no output. "
            "The code did not call print() to display any results. "
            "Please rewrite the code using print() for all results. "
            "Example: print(df['Facility level'].value_counts())"
        )

    tables = state_updates.get('tables') or []

    # FIX: Surface execution errors to the agent (prevents blind retries)
    # If executor encountered errors, append them to the output so LLM sees them
    if state_updates.get('errors'):
        error_text = "\n\n⚠️ **Execution Error:**\n" + "\n".join(state_updates['errors'])

        # Include code snippet and fix hints for common errors
        if "list indices must be integers" in error_text:
            error_text += "\n\n**FIX**: You likely passed df['column'] instead of 'column' to plotly."
            error_text += "\nUse: `px.histogram(df, x='column_name')` NOT `px.histogram(df, x=df['column_name'])`"

        # Include the code that failed so the LLM can learn
        error_text += f"\n\n**Code that failed:**\n```python\n{python_code}\n```"

        formatted_output += error_text
        logger.info(f"Appended error text to output: {error_text[:100]}...")

    # Validate output to prevent hallucinations and impossible values
    from app.agent.data_validator import DataValidator
    
    # Build DYNAMIC context for validation - works with ANY dataset
    validation_context = {}
    if 'df' in current_data and hasattr(current_data['df'], 'columns'):
        df = current_data['df']
        
        # DYNAMIC: Find ALL string/object columns that might contain entity names
        # This works for ANY dataset - facilities, companies, products, locations, etc.
        for col in df.columns:
            if df[col].dtype == 'object':  # String columns
                unique_values = df[col].dropna().unique()
                # If column has reasonable number of unique values, it might be entity names
                if 2 <= len(unique_values) <= 1000:  # Reasonable range for entities
                    # Store all unique values for validation
                    if 'entity_names' not in validation_context:
                        validation_context['entity_names'] = []
                    validation_context['entity_names'].extend(unique_values.tolist())
                    logger.debug(f"Added {len(unique_values)} unique values from column '{col}' for validation")
        
        # DYNAMIC: Store numeric column ranges for validation
        validation_context['numeric_ranges'] = {}
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64']:
                validation_context['numeric_ranges'][col] = {
                    'min': df[col].min(),
                    'max': df[col].max()
                }
    
    # Validate the output
    is_valid, issues = DataValidator.validate_output(formatted_output, validation_context)
    
    if not is_valid:
        logger.warning(f"Output validation failed: {issues}")
        # Sanitize the output to remove hallucinations
        formatted_output = DataValidator.sanitize_output(formatted_output, validation_context)

    if tables:
        table_sections = []
        for table in tables:
            name = table.get('name') or 'Table'
            markdown = table.get('markdown') or ''
            csv_path = table.get('csv_path')
            row_count = table.get('row_count')
            col_count = table.get('column_count')

            section_lines = [f"### {name}", f"Rows: {row_count}, Columns: {col_count}"]
            if markdown:
                section_lines.append("\n" + markdown)
            if csv_path:
                section_lines.append(f"\nDownload CSV: {csv_path}")
            table_sections.append("\n".join(section_lines))

        if table_sections:
            formatted_output = formatted_output.strip()
            if formatted_output:
                formatted_output += "\n\n"
            formatted_output += "\n\n".join(table_sections)

        # Don't provide fallback - return the formatted output even with issues
        # The health officials need to see the actual data, not error messages
        # if len(issues) > 2:
        #     logger.error(f"Too many validation issues ({len(issues)}), providing fallback response")
        #     return "I encountered issues generating accurate results. Let me try a different approach. Please ensure I'm working with the correct data columns."

    # Attach debug block for frontend observability (feature-flagged via CHATMRPT_DEBUG)
    try:
        if os.getenv('CHATMRPT_DEBUG', '0') != '0':
            debug = {}
            # Data context
            if 'df' in current_data and isinstance(current_data['df'], pd.DataFrame):
                df = current_data['df']
                debug.update({
                    'df_shape': list(df.shape),
                    'canonical_columns_count': len(current_data.get('canonical_columns') or {}),
                })
            # Code + plots
            debug['code_length'] = len(python_code or '')
            debug['plots_count'] = len(state_updates.get('output_plots') or [])
            # Timings
            exec_ms = state_updates.get('executor_ms')
            if isinstance(exec_ms, (int, float)):
                timings = {'executor_ms': int(exec_ms)}
            else:
                timings = {'executor_ms': int((pd.Timestamp.utcnow() - _exec_start).total_seconds() * 1000)}
            debug['timings'] = timings
            # Errors / validation issues count if available later
            if 'errors' in state_updates:
                debug['validation_issues_count'] = len(state_updates.get('errors') or [])
            # Attach
            state_updates['debug'] = debug
    except Exception:
        pass

    # Return tuple matching AgenticDataAnalysis pattern (output, state_updates)
    return formatted_output, state_updates


def get_data_summary(session_id: str) -> str:
    """
    Get a summary of available data for the session.
    Uses cached metadata for instant response, avoiding loading large files.
    
    Args:
        session_id: User session ID
        
    Returns:
        String description of available data
    """
    # First, try to get summary from cache (instant!)
    cached_summary = MetadataCache.get_summary_from_cache(session_id)
    if cached_summary:
        logger.info(f"Using cached metadata for session {session_id}")
        return cached_summary
    
    # Fallback: No cache exists, generate metadata on the fly (but with sampling)
    logger.info(f"No metadata cache found for session {session_id}, generating...")
    
    data_dir = f"instance/uploads/{session_id}"
    if not os.path.exists(data_dir):
        return "No data files uploaded yet."
    
    files = os.listdir(data_dir)
    # Filter for data files, excluding metadata cache
    data_files = [f for f in files if f.endswith(('.csv', '.xlsx', '.xls', '.json')) 
                  and f != 'metadata_cache.json']
    
    if not data_files:
        return "No data files uploaded yet."
    
    # Generate metadata for all files and cache it
    cache = {'files': {}}
    summary_parts = ["Available data:"]
    
    for filename in data_files:
        filepath = os.path.join(data_dir, filename)
        try:
            # Extract metadata (with sampling for large files)
            metadata = MetadataCache.extract_file_metadata(filepath, filename)
            cache['files'][filename] = metadata
            
            # Format summary
            if metadata.get('type') in ['csv', 'excel']:
                rows = metadata.get('rows', 'Unknown')
                cols = metadata.get('columns', 'Unknown')
                
                if isinstance(rows, int):
                    if metadata.get('rows_estimated'):
                        rows_str = f"~{rows:,}"
                    else:
                        rows_str = f"{rows:,}"
                else:
                    rows_str = str(rows)
                
                var_name = filename.split('.')[0].replace(' ', '_').replace('-', '_')
                size_mb = metadata.get('file_size_mb', 0)
                size_info = f" ({size_mb:.1f}MB)" if size_mb > 10 else ""
                
                summary_parts.append(f"- {var_name}: {rows_str} rows, {cols} columns{size_info}")
                
                if metadata.get('is_sampled'):
                    summary_parts.append(f"  (Metadata from sampling)")
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            summary_parts.append(f"- {filename}: Error reading file")
    
    # Save the cache for next time
    from datetime import datetime
    cache['last_updated'] = datetime.now().isoformat()
    MetadataCache.save_cache(session_id, cache)
    
    return "\n".join(summary_parts)
