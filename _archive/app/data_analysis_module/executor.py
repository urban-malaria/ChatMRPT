"""
Data Analysis Executor - Isolated from main application
Following Open Interpreter and Data Interpreter patterns
"""

import pandas as pd
import numpy as np
import io
import sys
import json
import logging
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class DataExecutor:
    """
    Minimal LLM-powered data executor - completely isolated module.
    Can be tested independently without affecting main ChatMRPT functionality.
    """
    
    def __init__(self, llm_manager=None):
        """
        Initialize with optional LLM manager.
        Can work standalone for testing.
        """
        self.llm = llm_manager
        self.data_cache = {}
        self.analysis_history = []
        
    def analyze(self, file_path: str, query: str, session_id: str = 'default') -> Dict[str, Any]:
        """
        Main analysis entry point - completely independent operation.
        """
        try:
            logger.info(f"Starting analysis - file_path: {file_path}, session_id: {session_id}")
            
            # Check if file exists
            from pathlib import Path
            if not Path(file_path).exists():
                logger.error(f"File not found: {file_path}")
                return {
                    'success': False,
                    'error': f"File not found: {file_path}",
                    'session_id': session_id
                }
            
            # Load data - could be single DataFrame or dict of DataFrames
            data = self._load_all_data(file_path)
            self.data_cache[session_id] = data
            
            # Build context based on data type
            if isinstance(data, dict):
                # Multiple sheets/dataframes
                context = self._build_multi_context(data, query)
                # Pass all dataframes to execution
                exec_data = data
            else:
                # Single dataframe
                context = self._build_context(data, query)
                exec_data = data
            
            # Generate code using LLM - REQUIRED!
            if not self.llm:
                logger.error("No LLM available - cannot perform analysis!")
                return {
                    'success': False,
                    'error': 'No LLM configured. Data analysis requires an LLM to generate analysis code.',
                    'session_id': session_id
                }
            
            logger.info("Using LLM to generate code")
            code = self._generate_code(context)
            logger.info(f"Generated code (first 500 chars): {code[:500]}")
            
            # Execute safely
            result = self._execute_code_with_data(code, exec_data)
            
            # Debug logging
            logger.info(f"Execution result - output length: {len(result.get('output', ''))}")
            logger.info(f"Execution result - has error: {bool(result.get('error'))}")
            if result.get('error'):
                logger.error(f"Execution error: {result['error']}")
                # NO FALLBACK - show the error
            
            # Store in history
            self.analysis_history.append({
                'query': query,
                'code': code,
                'result': result
            })
            
            return {
                'success': True,
                'code': code,
                'output': result.get('output', ''),
                'figures': result.get('figures', []),
                'error': result.get('error'),
                'session_id': session_id
            }
            
        except Exception as e:
            logger.error(f"Data analysis error: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'session_id': session_id
            }
    
    def _load_all_data(self, file_path: str):
        """Load ALL data from file - returns DataFrame or dict of DataFrames."""
        logger.info(f"_load_all_data called with: {file_path}")
        
        # Convert to Path object first
        file_path = Path(file_path)
        
        # If it's a relative path, make it absolute from the project root
        if not file_path.is_absolute():
            # Assume we're running from the project root
            file_path = Path.cwd() / file_path
        
        logger.info(f"Resolved path: {file_path}")
        logger.info(f"File exists: {file_path.exists()}")
        logger.info(f"Current working directory: {Path.cwd()}")
        
        if not file_path.exists():
            # Try one more time with just the filename in case it's in current directory
            just_filename = Path(file_path).name
            logger.error(f"File not found at {file_path}, trying {just_filename}")
            if Path(just_filename).exists():
                file_path = Path(just_filename)
            else:
                raise FileNotFoundError(f"File not found: {file_path}")
            
        ext = file_path.suffix.lower()
        logger.info(f"File extension: {ext}")
        
        if ext == '.csv':
            return pd.read_csv(file_path)
        elif ext in ['.xlsx', '.xls']:
            # For Excel files, read ALL sheets and return as dict
            try:
                xl = pd.ExcelFile(file_path)
                
                # If single sheet, just return DataFrame
                if len(xl.sheet_names) == 1:
                    return pd.read_excel(file_path)
                
                # Multiple sheets - return ALL as dict for LLM to access
                all_sheets = {}
                for sheet_name in xl.sheet_names:
                    try:
                        df = pd.read_excel(file_path, sheet_name=sheet_name)
                        # Only include non-empty sheets
                        if not df.empty:
                            all_sheets[sheet_name] = df
                            logger.info(f"Loaded sheet '{sheet_name}' with shape {df.shape}")
                    except Exception as e:
                        logger.warning(f"Could not read sheet '{sheet_name}': {e}")
                        continue
                
                if all_sheets:
                    logger.info(f"Excel file has {len(all_sheets)} sheets available for analysis")
                    return all_sheets
                else:
                    # Fallback to default pandas behavior
                    return pd.read_excel(file_path)
                    
            except Exception as e:
                logger.warning(f"Error reading Excel: {e}")
                return pd.read_excel(file_path)
        elif ext == '.json':
            return pd.read_json(file_path)
        elif ext == '.parquet':
            return pd.read_parquet(file_path)
        else:
            # Try CSV as fallback
            return pd.read_csv(file_path)
    
    def _load_data(self, file_path: str) -> pd.DataFrame:
        """Legacy method - loads single DataFrame for backward compatibility."""
        data = self._load_all_data(file_path)
        if isinstance(data, dict):
            # Return the sheet with most data
            best_sheet = max(data.items(), 
                           key=lambda x: x[1].shape[0] * x[1].shape[1])
            logger.info(f"Returning sheet '{best_sheet[0]}' for legacy compatibility")
            return best_sheet[1]
        return data
    
    def _build_context(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Build analysis context for LLM - single DataFrame."""
        return {
            'shape': df.shape,
            'columns': df.columns.tolist(),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'head': df.head(3).to_dict('records'),
            'query': query,
            'null_counts': df.isnull().sum().to_dict(),
            'numeric_cols': df.select_dtypes(include=[np.number]).columns.tolist(),
            'object_cols': df.select_dtypes(include=['object']).columns.tolist()
        }
    
    def _build_multi_context(self, sheets: Dict[str, pd.DataFrame], query: str) -> Dict[str, Any]:
        """Build analysis context for LLM - multiple DataFrames/sheets."""
        context = {
            'query': query,
            'sheets': {},
            'total_sheets': len(sheets)
        }
        
        for sheet_name, df in sheets.items():
            context['sheets'][sheet_name] = {
                'shape': df.shape,
                'columns': df.columns.tolist(),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'head': df.head(2).to_dict('records'),
                'null_counts': df.isnull().sum().to_dict(),
                'numeric_cols': df.select_dtypes(include=[np.number]).columns.tolist(),
                'object_cols': df.select_dtypes(include=['object']).columns.tolist()
            }
        
        return context
    
    def _generate_code(self, context: Dict[str, Any]) -> str:
        """Generate analysis code using LLM with production-grade prompting."""
        from .prompts import AnalysisPrompts
        
        # First, try to generate using our optimized prompt
        prompt = AnalysisPrompts.build_analysis_prompt(context)
        
        # Log the prompt for debugging
        logger.info("Prompt being sent to LLM (first 500 chars):")
        logger.info(prompt[:500])
        
        # Generate with lower temperature for more deterministic code
        if hasattr(self.llm, 'generate'):
            response = self.llm.generate(prompt, max_tokens=3000, temperature=0.1)
        else:
            # Fallback for LLMManagerWrapper
            response = self.llm.generate_response(prompt, max_tokens=3000, temperature=0.1)
        
        logger.info(f"LLM response type: {type(response)}")
        logger.info(f"LLM response (first 200 chars): {response[:200]}")
        
        # Clean the response - CRITICAL: Remove any markdown artifacts
        code = response
        
        # Remove markdown code blocks if present (shouldn't be, but defensive)
        if "```python" in code:
            logger.warning("Found ```python in response - LLM ignored instructions!")
            code = code.split("```python")[1].split("```")[0]
        elif "```" in code:
            logger.warning("Found ``` in response - LLM ignored instructions!")
            code = code.split("```")[1].split("```")[0]
        
        # Remove any leading/trailing markdown or text
        lines = code.strip().split('\n')
        
        # Find where actual Python code starts
        code_start = 0
        for i, line in enumerate(lines):
            # Look for Python code indicators
            if (line.strip().startswith('import ') or 
                line.strip().startswith('from ') or
                line.strip().startswith('print(') or
                line.strip().startswith('try:') or
                line.strip().startswith('if ') or
                line.strip().startswith('# ') and i > 0):  # Allow comments after code starts
                code_start = i
                break
        
        # Extract only the code portion
        if code_start > 0:
            logger.info(f"Trimming {code_start} non-code lines from beginning")
            code = '\n'.join(lines[code_start:])
        
        code = code.strip()
        
        # SAFETY CHECK: Detect and fix common LLM mistakes
        forbidden_patterns = [
            'pd.read_excel',
            'pd.read_csv',
            'pd.read_json',
            'pd.read_parquet',
            'open(',
            'with open',
            'pd.ExcelFile'
        ]
        
        for pattern in forbidden_patterns:
            if pattern in code:
                logger.warning(f"Generated code contains forbidden pattern '{pattern}' - auto-fixing...")
                # Auto-fix file reading attempts
                import re
                code = re.sub(r"df\s*=\s*pd\.read_excel\([^)]+\)", "# df is already loaded", code)
                code = re.sub(r"df\s*=\s*pd\.read_csv\([^)]+\)", "# df is already loaded", code)
                code = re.sub(r"data\s*=\s*pd\.read_excel\([^)]+\)", "# data is already loaded", code)
                code = re.sub(r"pd\.read_excel\([^)]+\)", "df", code)
                code = re.sub(r"pd\.read_csv\([^)]+\)", "df", code)
                break
        
        # NO FALLBACK - we want to see failures
        if not any(keyword in code for keyword in ['import', 'print', 'for', 'if', 'def', 'class', '=', '(', ')']):
            logger.error("LLM generated text instead of code! Will likely fail...")
            
        return code
    
    def _generate_mock_code(self, data, query: str) -> str:
        """Generate simple code for testing without LLM."""
        # Clean query for use in comment (remove newlines)
        clean_query = query.replace('\n', ' ').replace('\r', ' ')[:100]
        
        if isinstance(data, dict):
            # Multiple sheets
            code = f"""# Analysis for: {clean_query}...
# Found {len(data)} sheets in the Excel file
"""
            for sheet_name in data.keys():
                code += f"""
print("\\n{'='*50}")
print(f"Sheet: {sheet_name}")
print("{'='*50}")
df = data['{sheet_name}']
print(f"Shape: {{df.shape}}")
print(f"Columns: {{df.columns.tolist()}}")
if not df.empty:
    print("\\nFirst few rows:")
    print(df.head(3))
    print("\\nData types:")
    print(df.dtypes)
"""
            return code
        else:
            # Single dataframe
            return f"""# Analysis for: {clean_query}...
print("Data shape:", df.shape)
print("\\nColumns:", df.columns.tolist())
print("\\nBasic statistics:")
print(df.describe())
print("\\nNull values:")
print(df.isnull().sum())"""
    
    def _execute_code_with_data(self, code: str, data) -> Dict[str, Any]:
        """Execute code in isolated environment with proper data setup."""
        output = io.StringIO()
        error_output = io.StringIO()
        figures = []
        error = None
        
        # Setup execution environment
        exec_globals = {
            'pd': pd,
            'np': np,
            '__builtins__': __builtins__,
            'print': lambda *args, **kwargs: print(*args, **kwargs, file=output),
            'locals': locals  # Allow code to check what variables exist
        }
        
        # Add data - either single df or dict of dfs
        if isinstance(data, dict):
            # Multiple sheets - provide ALL the variables the prompt promises
            exec_globals['data'] = data
            exec_globals['sheets'] = data  # Alias
            
            # Create individual dataframe variables for each sheet
            for sheet_name, sheet_df in data.items():
                # Create safe variable names
                safe_name = sheet_name.replace(' ', '_').replace('-', '_').replace('.', '_')
                exec_globals[f'df_{safe_name}'] = sheet_df
            
            logger.info(f"Setup multi-sheet environment with {len(data)} sheets")
            logger.info(f"Available variables: data, sheets, {', '.join([f'df_{s.replace(\" \", \"_\")}' for s in data.keys()])}")
        else:
            # Single dataframe - simple case
            exec_globals['df'] = data
            exec_globals['data'] = data  # Alias for compatibility
        
        # Add visualization libraries if available
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            exec_globals['plt'] = plt
            exec_globals['sns'] = sns
            
            # Clear any existing plots
            plt.close('all')
        except ImportError:
            pass
        
        try:
            with redirect_stdout(output), redirect_stderr(error_output):
                exec(code, exec_globals)
                
            # Capture any created figures
            try:
                import matplotlib.pyplot as plt
                import base64
                from io import BytesIO
                
                for fig_num in plt.get_fignums():
                    fig = plt.figure(fig_num)
                    buf = BytesIO()
                    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                    buf.seek(0)
                    fig_b64 = base64.b64encode(buf.read()).decode('utf-8')
                    figures.append(fig_b64)
                    plt.close(fig)
            except ImportError:
                pass
                
        except SyntaxError as e:
            # Syntax error means LLM generated bad code
            import traceback
            error = f"Code syntax error: {str(e)}\n\nThis usually means the LLM generated text instead of code."
            logger.error(f"Syntax error in generated code: {error}")
            # NO FALLBACK - let it fail
                
        except Exception as e:
            import traceback
            error = f"{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            logger.error(f"Execution error: {error}")
            
        return {
            'output': output.getvalue(),
            'error': error,
            'stderr': error_output.getvalue(),
            'figures': figures
        }
    
    def get_history(self, session_id: str = 'default') -> list:
        """Get analysis history for session."""
        return self.analysis_history
    
    def clear_cache(self, session_id: str = 'default'):
        """Clear data cache for session."""
        if session_id in self.data_cache:
            del self.data_cache[session_id]
    
    def _generate_fallback_summary(self, data):
        """Generate a simple fallback when LLM code fails."""
        try:
            output = []
            output.append("📊 Your Data Has Been Successfully Loaded!")
            output.append("=" * 50)
            
            if isinstance(data, dict):
                output.append(f"\nI found {len(data)} worksheets in your Excel file.")
                for sheet_name, df in data.items():
                    output.append(f"• {sheet_name}: {df.shape[0]:,} rows × {df.shape[1]} columns")
            else:
                output.append(f"\nYour dataset contains {data.shape[0]:,} records with {data.shape[1]} fields.")
            
            output.append("\n💡 What would you like to know about your data?")
            output.append("\nYou can ask me anything - I'll analyze it for you!")
            
            return '\n'.join(output)
        except Exception as e:
            logger.error(f"Error generating fallback summary: {e}")
            return "Your data has been loaded successfully! What would you like to know about it?"