"""
Conversational Data Access for ChatMRPT
Enables unlimited conversational data exploration with safe code execution

This service provides:
- Direct pandas/numpy code execution on session data
- Comprehensive data schema for LLM context
- Stage-aware data access (pre/post analysis)
- Safe code execution environment
- Universal query capabilities
"""

import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import base64
import sys
import traceback
from typing import Dict, Any, Optional, List, Tuple, Union
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr
import warnings
warnings.filterwarnings('ignore')

from ..data.flexible_data_access import FlexibleDataAccess
from ..tools.base import get_session_unified_dataset

logger = logging.getLogger(__name__)

class ConversationalDataAccess:
    """
    Conversational data access for ChatMRPT.
    
    Provides unlimited conversational data exploration through:
    - Direct pandas/numpy operations
    - Safe code execution
    - Comprehensive data schema
    - Stage-aware data access
    """
    
    def __init__(self, session_id: str, llm_manager=None):
        self.session_id = session_id
        self.llm_manager = llm_manager
        self.flexible_data_access = FlexibleDataAccess(session_id)
        
        # Available namespaces for code execution
        self.safe_globals = {
            'pd': pd,
            'np': np,
            'plt': plt,
            'sns': sns,
            'len': len,
            'sum': sum,
            'max': max,
            'min': min,
            'round': round,
            'abs': abs,
            'sorted': sorted,
            'list': list,
            'dict': dict,
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'print': print,
            'range': range,
            'enumerate': enumerate,
            'zip': zip,
        }
        
        # Execution results cache
        self.execution_cache = {}
    
    def get_analysis_stage(self) -> str:
        """Determine current analysis stage based on available files."""
        try:
            session_folder = Path(f"instance/uploads/{self.session_id}")
            
            if not session_folder.exists():
                return 'no_data'
            
            # Check for CSV data
            csv_files = [
                session_folder / "raw_data.csv",
                session_folder / "processed_data.csv"
            ]
            
            has_csv = any(f.exists() for f in csv_files)
            
            if not has_csv:
                return 'no_data'
            
            # Check for analysis results (post-analysis files)
            analysis_files = [
                session_folder / "unified_dataset.csv",
                session_folder / "analysis_results.csv", 
                session_folder / "analysis_cleaned_data.csv",
                session_folder / "composite_analysis.csv"
            ]
            
            has_analysis = any(f.exists() for f in analysis_files)
            
            # If analysis files exist, we're in post-analysis stage
            if has_analysis:
                return 'post_analysis'
            else:
                return 'pre_analysis'
                
        except Exception as e:
            logger.error(f"Error determining analysis stage: {e}")
            return 'no_data'
    
    def get_available_data(self) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """Get available data based on current analysis stage."""
        stage = self.get_analysis_stage()
        
        if stage == 'no_data':
            return None, {'error': 'No data available'}
        
        elif stage == 'pre_analysis':
            # Get raw combined data from session folder
            try:
                df = self._load_session_data('pre_analysis')
                if df is None:
                    return None, {'error': 'Failed to load raw data from session folder'}
                
                # Add basic info
                info = {
                    'stage': 'pre_analysis',
                    'data_type': 'raw_combined',
                    'shape': df.shape,
                    'columns': df.columns.tolist(),
                    'analysis_results_available': False
                }
                
                return df, info
                
            except Exception as e:
                logger.error(f"Error loading pre-analysis data: {e}")
                return None, {'error': str(e)}
        
        elif stage == 'post_analysis':
            # Get unified dataset with analysis results from session folder
            try:
                df = self._load_session_data('post_analysis')
                if df is None:
                    return None, {'error': 'Failed to load unified dataset from session folder'}
                
                # Add comprehensive info
                info = {
                    'stage': 'post_analysis',
                    'data_type': 'unified_dataset',
                    'shape': df.shape,
                    'columns': df.columns.tolist(),
                    'analysis_results_available': True,
                    'has_composite_scores': 'composite_score' in df.columns or 'Composite_Score' in df.columns,
                    'has_pca_scores': 'pca_score' in df.columns or 'PCA_Score' in df.columns,
                    'has_rankings': any('rank' in col.lower() for col in df.columns)
                }
                
                return df, info
                
            except Exception as e:
                logger.error(f"Error loading post-analysis data: {e}")
                return None, {'error': str(e)}
        
        return None, {'error': 'Unknown stage'}
    
    def _load_session_data(self, stage: str) -> Optional[pd.DataFrame]:
        """Load data from session folder based on stage."""
        session_folder = Path(f"instance/uploads/{self.session_id}")
        
        if not session_folder.exists():
            logger.error(f"Session folder not found: {session_folder}")
            return None
        
        if stage == 'pre_analysis':
            # Look for raw processed data
            possible_files = [
                session_folder / "processed_data.csv",
                session_folder / "raw_data.csv"
            ]
            
            for file_path in possible_files:
                if file_path.exists():
                    logger.info(f"Loading pre-analysis data from: {file_path}")
                    try:
                        return pd.read_csv(file_path)
                    except Exception as e:
                        logger.error(f"Error loading {file_path}: {e}")
                        continue
            
            logger.error("No suitable pre-analysis data file found")
            return None
            
        elif stage == 'post_analysis':
            # Look for unified dataset or analysis results
            possible_files = [
                session_folder / "unified_dataset.csv",
                session_folder / "analysis_results.csv",
                session_folder / "analysis_cleaned_data.csv",
                session_folder / "composite_analysis.csv"
            ]
            
            # Try to load from existing files
            for file_path in possible_files:
                if file_path.exists():
                    logger.info(f"Loading post-analysis data from: {file_path}")
                    try:
                        return pd.read_csv(file_path)
                    except Exception as e:
                        logger.error(f"Error loading {file_path}: {e}")
                        continue
            
            # If no unified dataset exists, try to use the existing function
            logger.info("No unified dataset file found, trying get_session_unified_dataset")
            try:
                return get_session_unified_dataset(self.session_id)
            except Exception as e:
                logger.error(f"Error loading unified dataset: {e}")
                return None
        
        return None
    
    def generate_comprehensive_schema(self) -> Dict[str, Any]:
        """Generate comprehensive data schema for LLM context."""
        df, info = self.get_available_data()
        
        if df is None:
            return {'error': info.get('error', 'No data available')}
        
        schema = {
            'dataset_info': {
                'stage': info['stage'],
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'data_type': info['data_type'],
                'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
            },
            'columns': {},
            'available_operations': self._get_available_operations(info['stage']),
            'sample_data': df.head(3).to_dict('records'),
            'malaria_context': self._get_malaria_context(df, info['stage'])
        }
        
        # Analyze each column
        for col in df.columns:
            schema['columns'][col] = self._analyze_column(df, col, info['stage'])
        
        return schema
    
    def _analyze_column(self, df: pd.DataFrame, col: str, stage: str) -> Dict[str, Any]:
        """Analyze individual column for schema."""
        try:
            series = df[col]
            
            analysis = {
                'dtype': str(series.dtype),
                'non_null_count': series.notna().sum(),
                'null_count': series.isnull().sum(),
                'null_percentage': round((series.isnull().sum() / len(df)) * 100, 2),
                'unique_count': series.nunique(),
                'malaria_relevance': self._assess_malaria_relevance(col, series, stage)
            }
            
            if pd.api.types.is_numeric_dtype(series):
                analysis.update({
                    'category': 'numeric',
                    'min': float(series.min()) if series.notna().any() else None,
                    'max': float(series.max()) if series.notna().any() else None,
                    'mean': float(series.mean()) if series.notna().any() else None,
                    'median': float(series.median()) if series.notna().any() else None,
                    'std': float(series.std()) if series.notna().any() else None,
                    'quartiles': {
                        'q25': float(series.quantile(0.25)) if series.notna().any() else None,
                        'q50': float(series.quantile(0.50)) if series.notna().any() else None,
                        'q75': float(series.quantile(0.75)) if series.notna().any() else None
                    }
                })
            else:
                analysis.update({
                    'category': 'categorical',
                    'top_values': series.value_counts().head(5).to_dict(),
                    'unique_values_sample': series.unique()[:10].tolist()
                })
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing column {col}: {e}")
            return {'error': str(e)}
    
    def _assess_malaria_relevance(self, col: str, series: pd.Series, stage: str) -> Dict[str, Any]:
        """Assess malaria relevance of a column."""
        col_lower = col.lower()
        
        # Define malaria keywords by category
        malaria_keywords = {
            'health_direct': ['malaria', 'fever', 'parasite', 'case', 'incidence', 'prevalence'],
            'health_intervention': ['itn', 'bed_net', 'irs', 'spray', 'treatment', 'clinic', 'healthcare'],
            'environmental': ['rainfall', 'temperature', 'humidity', 'water', 'stagnant', 'breeding'],
            'socioeconomic': ['poverty', 'income', 'education', 'wealth', 'economic'],
            'demographic': ['population', 'density', 'age', 'household'],
            'geographic': ['ward', 'district', 'state', 'region', 'area', 'urban', 'rural'],
            'analysis_results': ['composite', 'pca', 'score', 'rank', 'vulnerability', 'risk']
        }
        
        relevance = {
            'category': 'unknown',
            'score': 0,
            'reasoning': []
        }
        
        # Check for direct matches
        for category, keywords in malaria_keywords.items():
            if any(keyword in col_lower for keyword in keywords):
                relevance['category'] = category
                
                if category == 'health_direct':
                    relevance['score'] = 10
                    relevance['reasoning'].append("Direct malaria health indicator")
                elif category == 'health_intervention':
                    relevance['score'] = 9
                    relevance['reasoning'].append("Malaria intervention indicator")
                elif category == 'environmental':
                    relevance['score'] = 8
                    relevance['reasoning'].append("Environmental factor affecting malaria transmission")
                elif category == 'analysis_results':
                    relevance['score'] = 7 if stage == 'post_analysis' else 0
                    relevance['reasoning'].append("Analysis result variable")
                elif category == 'socioeconomic':
                    relevance['score'] = 6
                    relevance['reasoning'].append("Socioeconomic factor influencing malaria risk")
                elif category == 'demographic':
                    relevance['score'] = 5
                    relevance['reasoning'].append("Demographic factor")
                elif category == 'geographic':
                    relevance['score'] = 4
                    relevance['reasoning'].append("Geographic identifier")
                
                break
        
        # Additional analysis for unknown categories
        if relevance['category'] == 'unknown':
            if pd.api.types.is_numeric_dtype(series):
                relevance['score'] = 3
                relevance['reasoning'].append("Numeric variable - potentially useful for analysis")
            else:
                relevance['score'] = 2
                relevance['reasoning'].append("Categorical variable - may be identifier or grouping")
        
        return relevance
    
    def _get_available_operations(self, stage: str) -> List[str]:
        """Get available operations based on stage."""
        base_operations = [
            'df.describe()',
            'df.info()',
            'df.head()',
            'df.tail()',
            'df.columns',
            'df.shape',
            'df.dtypes',
            'df.isnull().sum()',
            # 'df.corr()',  # Excluded to avoid string column errors
            'df.groupby()',
            'df.value_counts()',
            'df.plot()',
            'df.hist()',
            'df.boxplot()',
            'sns.heatmap()',
            'sns.scatterplot()',
            'sns.barplot()'
        ]
        
        if stage == 'post_analysis':
            base_operations.extend([
                'df.nlargest()',
                'df.nsmallest()',
                'df.rank()',
                'df.query()',
                'df.sort_values()',
                'Method comparison operations',
                'Ward ranking analysis',
                'Risk score analysis'
            ])
        
        return base_operations
    
    def _get_malaria_context(self, df: pd.DataFrame, stage: str) -> Dict[str, Any]:
        """Get malaria-specific context for the dataset."""
        context = {
            'domain': 'malaria_epidemiology',
            'use_case': 'urban_microstratification',
            'geographic_focus': 'nigeria',
            'analysis_purpose': 'intervention_targeting'
        }
        
        # Identify key variables
        health_vars = [col for col in df.columns if any(term in col.lower() 
                      for term in ['malaria', 'fever', 'case', 'itn', 'healthcare', 'clinic'])]
        
        env_vars = [col for col in df.columns if any(term in col.lower() 
                   for term in ['rainfall', 'temperature', 'water', 'humidity'])]
        
        socio_vars = [col for col in df.columns if any(term in col.lower() 
                     for term in ['poverty', 'education', 'income', 'wealth'])]
        
        geo_vars = [col for col in df.columns if any(term in col.lower() 
                   for term in ['ward', 'district', 'state', 'region', 'area'])]
        
        context['variable_categories'] = {
            'health_indicators': health_vars,
            'environmental_factors': env_vars,
            'socioeconomic_factors': socio_vars,
            'geographic_identifiers': geo_vars
        }
        
        if stage == 'post_analysis':
            analysis_vars = [col for col in df.columns if any(term in col.lower() 
                           for term in ['composite', 'pca', 'score', 'rank', 'vulnerability'])]
            context['variable_categories']['analysis_results'] = analysis_vars
        
        return context
    
    def execute_code(self, code: str, context: str = "") -> Dict[str, Any]:
        """Execute pandas/numpy code safely on session data."""
        
        try:
            # Validate code safety first
            if not self._is_code_safe(code):
                return {
                    'success': False,
                    'error': 'Code contains potentially dangerous operations',
                    'output': '',
                    'code': code
                }
            
            # Get current data
            df, info = self.get_available_data()
            if df is None:
                logger.error(f"No data available for session {self.session_id}: {info}")
                return {
                    'success': False,
                    'error': info.get('error', 'No data available'),
                    'output': '',
                    'code': code
                }
            
            logger.info(f"Data loaded successfully: {df.shape} rows x columns")
            
            # Set up execution environment - merge locals into globals for better accessibility
            execution_globals = self.safe_globals.copy()
            execution_globals.update({
                'df': df,
                'data': df,  # Alternative name
                'session_id': self.session_id,
                'stage': info['stage']
            })
            
            # Capture output
            output_buffer = io.StringIO()
            error_buffer = io.StringIO()
            
            # Execute code with output capture
            with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
                try:
                    # Execute the code with merged namespace
                    exec(code, execution_globals)
                    
                    # Get output
                    output = output_buffer.getvalue()
                    error_output = error_buffer.getvalue()
                    
                    # Handle matplotlib plots
                    plot_data = None
                    if plt.get_fignums():  # Check if any figures exist
                        plot_data = self._capture_plot()
                    
                    # Format the output for better user experience
                    formatted_output = self._format_analysis_output(output, code, context)
                    
                    result = {
                        'success': True,
                        'output': formatted_output,
                        'raw_output': output,  # Keep raw output for debugging
                        'error': error_output,
                        'plot_data': plot_data,
                        'code': code,
                        'context': context,
                        'variables_created': []  # Not tracking new variables with merged namespace
                    }
                    
                    return result
                    
                except Exception as e:
                    return {
                        'success': False,
                        'error': str(e),
                        'traceback': traceback.format_exc(),
                        'output': output_buffer.getvalue(),
                        'code': code
                    }
            
        except Exception as e:
            logger.error(f"Error executing code: {e}")
            return {
                'success': False,
                'error': f"Execution error: {str(e)}",
                'output': '',
                'code': code
            }
    
    def _is_code_safe(self, code: str) -> bool:
        """Check if code is safe to execute."""
        # Convert to lowercase for case-insensitive checking
        code_lower = code.lower()
        
        # More comprehensive list of dangerous patterns
        dangerous_patterns = [
            # System operations
            'os.system', 'os.popen', 'os.spawn', 'os.exec', 'os.fork',
            'subprocess.', 'popen', 'system(',
            
            # File operations
            'open(', 'file(', 'io.open', 'pathlib.path', 'with open',
            
            # Dynamic code execution
            'exec(', 'eval(', 'compile(', '__import__',
            
            # Input/output
            'input(', 'raw_input(',
            
            # Network operations
            'socket.', 'urllib', 'requests.', 'http.', 'ftp.',
            
            # Introspection and modification
            'globals()', 'locals()', 'vars()', 'dir(',
            'getattr', 'setattr', 'delattr', 'hasattr',
            
            # System modules
            'import os', 'import sys', 'import subprocess', 'import socket',
            'import shutil', 'import glob', 'import tempfile',
            
            # Process and thread operations
            'threading.', 'multiprocessing.', 'concurrent.',
            
            # Database operations
            'sqlite3.', 'psycopg2.', 'pymongo.',
            
            # Other risky operations
            'pickle.', 'marshal.', 'shelve.',
            'ctypes.', 'gc.', 'sys.exit', 'quit()', 'exit()',
            
            # Shell-like operations
            'rm ', 'del ', 'rmdir', 'mkdir', 'chmod', 'chown',
        ]
        
        # Check for dangerous patterns
        for pattern in dangerous_patterns:
            if pattern in code_lower:
                logger.warning(f"Unsafe code detected: {pattern}")
                return False
        
        # Check for dangerous imports using regex
        import re
        
        # Check for import statements with dangerous modules
        import_patterns = [
            r'import\s+os\b',
            r'import\s+sys\b', 
            r'import\s+subprocess\b',
            r'import\s+socket\b',
            r'import\s+shutil\b',
            r'from\s+os\s+import',
            r'from\s+sys\s+import',
            r'from\s+subprocess\s+import',
            r'__import__\s*\(',
        ]
        
        for pattern in import_patterns:
            if re.search(pattern, code_lower):
                logger.warning(f"Unsafe import detected: {pattern}")
                return False
        
        # Check for attribute access to dangerous modules
        # This catches cases like: pandas.os.system() or similar
        dangerous_attrs = [
            r'\.os\.',
            r'\.sys\.',
            r'\.subprocess\.',
            r'\.socket\.',
        ]
        
        for pattern in dangerous_attrs:
            if re.search(pattern, code_lower):
                logger.warning(f"Unsafe attribute access detected: {pattern}")
                return False
        
        return True
    
    def _capture_plot(self) -> Optional[str]:
        """Capture matplotlib plot as base64 string."""
        try:
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
            buffer.seek(0)
            
            # Convert to base64
            plot_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
            
            # Clear the plot
            plt.clf()
            plt.close('all')
            
            return plot_data
            
        except Exception as e:
            logger.error(f"Error capturing plot: {e}")
            return None
    
    def build_conversational_prompt(self, session_context: Dict[str, Any]) -> str:
        """Build conversational prompt with full data context."""
        schema = self.generate_comprehensive_schema()
        
        if 'error' in schema:
            return f"""You are ChatMRPT, but no data is currently available.
            
Error: {schema['error']}

Please ask the user to upload data first (CSV file with ward-level data and optional shapefile with geographic boundaries)."""
        
        stage = schema['dataset_info']['stage']
        
        prompt = f"""You are ChatMRPT, a malaria epidemiologist with DIRECT ACCESS to the user's dataset.

## DATA ACCESS CAPABILITIES

You have full pandas/numpy access to the dataset. You can execute ANY pandas operation by generating code.

### Current Data Status:
- **Stage**: {stage.upper()}
- **Dataset**: {schema['dataset_info']['total_rows']} rows, {schema['dataset_info']['total_columns']} columns
- **Memory Usage**: {schema['dataset_info']['memory_usage_mb']} MB
- **Data Type**: {schema['dataset_info']['data_type']}

### Available DataFrame: `df`
```python
# The dataset is available as 'df' - you can use any pandas operations:
df.head()          # View first 5 rows
df.describe()      # Statistical summary
df.info()          # Data types and missing values
# df.corr()          # Correlation matrix (use with numeric columns only)
df.groupby('column').mean()  # Group analysis
df.query('condition')        # Filter data
df.plot()          # Create visualizations
```

## DATASET SCHEMA

### Column Details:
"""
        
        # Add column information
        for col, info in schema['columns'].items():
            prompt += f"\n**{col}**:\n"
            prompt += f"- Type: {info['category']} ({info['dtype']})\n"
            prompt += f"- Completeness: {info['non_null_count']}/{schema['dataset_info']['total_rows']} ({100-info['null_percentage']:.1f}%)\n"
            prompt += f"- Unique values: {info['unique_count']}\n"
            prompt += f"- Malaria relevance: {info['malaria_relevance']['category']} (score: {info['malaria_relevance']['score']}/10)\n"
            
            if info['category'] == 'numeric' and info['mean'] is not None:
                prompt += f"- Range: {info['min']:.2f} to {info['max']:.2f} (mean: {info['mean']:.2f})\n"
            elif info['category'] == 'categorical':
                top_values = list(info['top_values'].keys())[:3]
                prompt += f"- Top values: {top_values}\n"
        
        # Add stage-specific capabilities
        if stage == 'pre_analysis':
            prompt += f"""
## PRE-ANALYSIS CAPABILITIES

You can help users explore their raw data:
- **Data Quality**: Check missing values, outliers, data types
- **Descriptive Statistics**: Summary statistics, distributions
- **Correlations**: Analyze relationships between variables
- **Visualizations**: Create plots to understand data patterns
- **Geographic Analysis**: Explore spatial patterns (if shapefile available)

### Example Queries You Can Answer:
- "What's the correlation between rainfall and malaria cases?"
- "Show me the distribution of ITN coverage"
- "Which variables have the most missing data?"
- "Create a scatter plot of population vs healthcare access"
- "What are the top 10 wards by population?"

**Note**: Analysis results (composite scores, PCA) are not available yet.
"""
        
        else:  # post_analysis
            prompt += f"""
## POST-ANALYSIS CAPABILITIES

You have access to complete analysis results:
- **Raw Data**: All original variables
- **Analysis Results**: Composite scores, PCA results, rankings
- **Risk Classifications**: Vulnerability categories
- **Method Comparisons**: Compare composite vs PCA approaches

### Analysis Results Available:
- **Composite Scores**: {schema['malaria_context']['variable_categories'].get('analysis_results', [])}
- **Ward Rankings**: Risk-based prioritization
- **Vulnerability Categories**: High/Medium/Low risk classification

### Example Queries You Can Answer:
- "What's the correlation between composite and PCA scores?"
- "Show me the top 10 highest risk wards"
- "Which wards have the biggest score differences between methods?"
- "Create a scatter plot of composite vs PCA scores"
- "What factors correlate most with high risk scores?"
- "Show intervention recommendations for high-risk wards"
"""
        
        prompt += f"""
## AVAILABLE OPERATIONS

You can perform these operations (and many more):
{chr(10).join(f"- {op}" for op in schema['available_operations'])}

## MALARIA CONTEXT

- **Domain**: Malaria epidemiology and urban microstratification
- **Purpose**: Intervention targeting for malaria control
- **Geographic Focus**: Nigeria (urban areas)
- **Key Variables**: {schema['malaria_context']['variable_categories']}

## RESPONSE GUIDELINES

1. **Direct Code Execution**: Generate pandas/numpy code to answer user questions
2. **Explain Results**: Interpret findings in malaria epidemiology context
3. **Actionable Insights**: Provide public health recommendations
4. **Visualizations**: Create plots when helpful for understanding
5. **Quantitative Details**: Always provide specific numbers and statistics

## SAMPLE DATA PREVIEW

Here's a sample of the data:
```python
{schema['sample_data']}
```

**Your Role**: You are a malaria epidemiologist with unlimited data access. Answer ANY question about the dataset using pandas operations. Be specific, quantitative, and provide epidemiological insights.
"""
        
        return prompt
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """Process natural language query by generating and executing code."""
        
        try:
            # Get data schema for context
            schema = self.generate_comprehensive_schema()
            if 'error' in schema:
                return {
                    'success': False,
                    'error': schema['error'],
                    'query': query
                }
            
            # Generate code using LLM
            if not self.llm_manager:
                return {
                    'success': False,
                    'error': 'LLM manager not available for query processing',
                    'query': query
                }
            
            # Build prompt for code generation
            code_prompt = f"""
Given this malaria dataset schema and a user query, generate Python pandas code to answer the question.

DATASET SCHEMA:
- Total rows: {schema['dataset_info']['total_rows']}
- Total columns: {schema['dataset_info']['total_columns']}
- Stage: {schema['dataset_info']['stage']}

AVAILABLE COLUMNS:
{', '.join(schema['columns'].keys())}

SAMPLE DATA:
{schema['sample_data']}

USER QUERY: {query}

Generate Python code using 'df' as the dataframe variable. If the query asks for visualization, use matplotlib/seaborn.

IMPORTANT:
- Use only pandas, numpy, matplotlib, seaborn operations
- The dataframe is already loaded as 'df'
- For plots, use plt.figure() and plt.show()
- ALWAYS use print() statements to show results
- For data exploration, use print() to display values, summaries, etc.
- Format numeric outputs to 4 decimal places when appropriate (e.g., .4f for floats)
- Return only the code, no explanations
- CRITICAL: For "check data quality" queries, DO NOT include correlation analysis - focus on missing values, data types, duplicates, and basic statistics only

EXAMPLES:
- For "What is the shape?": print(f"Dataset shape: {{df.shape[0]}} rows x {{df.shape[1]}} columns")
- For "Show min/max": print(f"Min value: {{df['column'].min():.4f}}\\nMax value: {{df['column'].max():.4f}}")
- For "Show correlation": print(df[['col1', 'col2']].corr().round(4))
- For "List top values": print(df.nlargest(10, 'column').round(4))

CODE:
"""
            
            # Generate code
            response = self.llm_manager.generate_response(
                prompt=code_prompt,
                temperature=0.1,
                max_tokens=500
            )
            
            generated_code = response.strip()
            
            # Clean up code (remove markdown formatting if present)
            if generated_code.startswith('```'):
                lines = generated_code.split('\n')
                generated_code = '\n'.join(lines[1:-1])
            
            logger.info(f"Generated code for query '{query}':\n{generated_code}")
            
            # Execute the generated code
            result = self.execute_code(generated_code, context=query)
            
            # Add query context to result
            result['query'] = query
            result['generated_code'] = generated_code
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return {
                'success': False,
                'error': f"Failed to process query: {str(e)}",
                'query': query
            }
    
    def process_sql_query(self, sql_query: str, original_query: str = "") -> Dict[str, Any]:
        """
        Process SQL query using DuckDB and unified formatting.

        Args:
            sql_query: The SQL query to execute
            original_query: The original natural language query (for better intent detection)

        Returns:
            Dictionary with success status, formatted output, and metadata
        """
        try:
            import duckdb
            from .query_intent_analyzer import QueryIntentAnalyzer
            from .unified_formatter import UnifiedFormatter
            from .query_result import QueryResult

            # Get the dataframe
            df, info = self.get_available_data()
            if df is None:
                return {
                    'success': False,
                    'error': 'No data available for SQL query',
                    'query': sql_query
                }

            # Create DuckDB connection and register the DataFrame
            conn = duckdb.connect(':memory:')
            conn.register('df', df)

            logger.info(f"Executing SQL query: {sql_query}")

            # Execute the SQL query
            result_df = conn.execute(sql_query).fetchdf()

            # Analyze intent and result type
            analyzer = QueryIntentAnalyzer()
            intent, result_type = analyzer.analyze_full(result_df, sql_query, original_query)

            # Create structured result
            query_result = QueryResult(
                data=result_df,
                sql_query=sql_query,
                original_query=original_query,
                intent=intent,
                result_type=result_type
            )

            # Format with unified formatter
            formatter = UnifiedFormatter()

            # For ranking explanations, pass the full dataset for percentile calculations
            if result_type.value == 'ranking' and len(result_df) == 1:
                output = formatter._format_ranking_explanation(query_result, full_df=df)
            else:
                output = formatter.format(query_result)

            # Check if visualization would be helpful
            plot_data = None
            if 'group by' in sql_query.lower() and len(result_df) <= 20:
                plot_data = {
                    'type': 'bar',
                    'data': result_df.to_dict('records'),
                    'query': sql_query
                }

            return {
                'success': True,
                'output': output,
                'plot_data': plot_data,
                'query': sql_query,
                'result_count': len(result_df),
                'intent': intent.value,
                'result_type': result_type.value
            }

        except Exception as e:
            logger.error(f"SQL query error: {str(e)}")

            if "no table" in str(e).lower():
                error_msg = "Error: Table 'df' not found. Make sure data is loaded."
            elif "syntax" in str(e).lower():
                error_msg = f"SQL syntax error: {str(e)}\n\nPlease check your SQL query syntax."
            else:
                error_msg = f"Error executing SQL query: {str(e)}"

            return {
                'success': False,
                'error': error_msg,
                'query': sql_query
            }
    
    def _format_analysis_output(self, output: str, code: str, context: str = "") -> str:
        """Format raw analysis output into structured, contextual responses."""
        if not output or not output.strip():
            return f"Analysis completed successfully. {context}"
        
        # Handle missing data reports first (before correlation check)
        if 'missing data' in context.lower() or 'Missing data in each column' in output:
            return self._format_missing_data_output(output, context)
        
        # Handle correlation matrices
        if 'correlation' in context.lower() or ('elevation' in output and 'pfpr' in output and 'corr' in context.lower()):
            return self._format_correlation_output(output, context)
        
        # Handle histogram/distribution requests
        if 'histogram' in context.lower() or 'distribution' in context.lower():
            return self._format_histogram_output(output, context)
        
        # Handle statistical summaries
        if 'describe' in context.lower() or 'statistics' in context.lower():
            return self._format_statistical_output(output, context)
        
        # Handle data quality reports
        if 'quality' in context.lower() or 'data types' in output:
            return self._format_data_quality_output(output, context)
        
        # Handle general queries with data output
        if output.strip():
            return self._format_general_output(output, context)
        
        return f"Analysis completed. {context}"
    
    def _format_correlation_output(self, output: str, context: str) -> str:
        """Format correlation matrix output."""
        lines = output.strip().split('\n')
        
        # Extract correlation value if present
        correlation_value = None
        if 'elevation' in output and 'pfpr' in output:
            # Look for correlation coefficient
            import re
            match = re.search(r'0\.\d+', output)
            if match:
                correlation_value = float(match.group())
        
        if correlation_value:
            # Interpret correlation strength
            if abs(correlation_value) < 0.1:
                strength = "very weak"
            elif abs(correlation_value) < 0.3:
                strength = "weak"
            elif abs(correlation_value) < 0.5:
                strength = "moderate"
            elif abs(correlation_value) < 0.7:
                strength = "strong"
            else:
                strength = "very strong"
            
            direction = "positive" if correlation_value > 0 else "negative"
            
            return f"""**Correlation Analysis Results**

The correlation between elevation and pfpr is {correlation_value:.3f}, indicating a {strength} {direction} relationship.

**Interpretation for Malaria Analysis:**
This {strength} correlation suggests that elevation {'has a modest influence on' if abs(correlation_value) > 0.15 else 'has minimal impact on'} malaria parasite prevalence in your study area. {'Higher elevations show slightly higher malaria rates' if correlation_value > 0 else 'Higher elevations show slightly lower malaria rates'}, which may be due to environmental factors like temperature, humidity, or vector breeding patterns at different altitudes.

**Public Health Implications:**
{'Consider elevation as a supplementary factor' if abs(correlation_value) > 0.15 else 'Elevation appears to be a minor factor'} when designing targeted interventions. Other environmental and socioeconomic variables may have stronger predictive power for malaria risk stratification."""
        
        return f"**Correlation Analysis**\n\n{output}\n\nThis analysis shows the relationships between variables in your malaria risk dataset."
    
    def _format_missing_data_output(self, output: str, context: str) -> str:
        """Format missing data analysis output."""
        lines = output.strip().split('\n')
        
        # Parse missing data counts
        missing_info = []
        total_missing = 0
        
        for line in lines:
            if line.strip() and not line.startswith('Missing data'):
                parts = line.strip().split()
                if len(parts) >= 2:
                    try:
                        count = int(parts[-1])
                        var_name = ' '.join(parts[:-1])
                        if count > 0:
                            missing_info.append((var_name, count))
                            total_missing += count
                    except ValueError:
                        continue
        
        if missing_info:
            # Sort by missing count
            missing_info.sort(key=lambda x: x[1], reverse=True)
            
            response = "**Data Quality Assessment**\n\n"
            
            if total_missing == 0:
                response += "Excellent news! Your dataset has no missing values across all variables. This high data quality will ensure robust analysis results for malaria risk assessment.\n\n"
            else:
                response += f"Your dataset has {total_missing} total missing values across {len(missing_info)} variables. Here's the breakdown:\n\n"
                
                for var_name, count in missing_info[:5]:  # Show top 5
                    response += f"• **{var_name}**: {count} missing values\n"
                
                if len(missing_info) > 5:
                    response += f"• ... and {len(missing_info) - 5} other variables with missing data\n"
            
            response += "\n**Recommendation for Malaria Analysis:**\n"
            if total_missing < 50:
                response += "The low amount of missing data suggests your dataset is well-suited for comprehensive malaria risk analysis. Consider using imputation techniques for critical variables if needed."
            else:
                response += "The missing data pattern should be carefully considered. Focus on variables with complete data for initial analysis, and consider data collection improvements for future studies."
            
            return response
        
        return f"**Data Quality Check**\n\n{output}\n\nThis shows the completeness of your malaria risk dataset."
    
    def _format_histogram_output(self, output: str, context: str) -> str:
        """Format histogram/distribution output."""
        if 'no output generated' in output:
            return f"""**Distribution Analysis Request**

I've generated a histogram visualization for you showing the distribution of pfpr values in your dataset. The chart should appear above this message.

**What to Look For:**
• **Distribution shape**: Is it normal, skewed, or bimodal?
• **Outliers**: Any unusual values that might need investigation?
• **Concentration**: Where do most pfpr values cluster?

**Malaria Analysis Context:**
Understanding pfpr (parasite prevalence) distribution helps identify:
- High-risk areas that need immediate intervention
- Patterns that might indicate environmental or social factors
- Data quality issues that could affect analysis results

If you don't see the chart, please let me know and I'll help troubleshoot the visualization."""
        
        return f"**Distribution Analysis**\n\n{output}\n\nThis shows the distribution pattern of values in your malaria dataset."
    
    def _format_statistical_output(self, output: str, context: str) -> str:
        """Format statistical summary output."""
        return f"""**Statistical Summary**

{output}

**Interpretation for Malaria Analysis:**
These statistics provide baseline understanding of your dataset's characteristics. Key insights for malaria risk assessment:

• **Central tendencies** help identify typical conditions in your study area
• **Variability measures** indicate how diverse the risk factors are across wards
• **Range information** shows the spectrum of conditions you're working with

Use these statistics to understand data distribution before proceeding with composite scoring or PCA analysis."""
    
    def _format_data_quality_output(self, output: str, context: str) -> str:
        """Format data quality assessment output."""
        lines = output.strip().split('\n')
        
        # Parse the output sections
        missing_count = 0
        duplicate_count = 0
        key_missing_vars = []
        
        # Parse missing values
        in_missing_section = False
        for line in lines:
            if 'Missing values per column:' in line:
                in_missing_section = True
                continue
            if 'Number of duplicate rows:' in line:
                in_missing_section = False
                try:
                    duplicate_count = int(line.split(':')[1].strip())
                except (ValueError, IndexError):
                    duplicate_count = 0
                continue
            if in_missing_section and line.strip():
                parts = line.strip().split()
                if len(parts) >= 2:
                    try:
                        var_name = ' '.join(parts[:-1])
                        count = int(parts[-1])
                        if count > 0:
                            missing_count += count
                            if count > 50:  # Significant missing data
                                key_missing_vars.append((var_name, count))
                    except (ValueError, IndexError):
                        continue
        
        # Build conversational response
        response = "**Data Quality Check Complete**\n\n"
        
        # Missing data summary
        if missing_count == 0:
            response += "✅ **Excellent!** Your dataset has complete data across all variables - no missing values found.\n\n"
        else:
            response += f"📊 Your dataset has {missing_count} total missing values"
            if key_missing_vars:
                response += " with significant gaps in:\n"
                for var, count in key_missing_vars[:3]:
                    response += f"• {var}: {count} missing entries\n"
            else:
                response += " (minimal impact on analysis).\n"
            response += "\n"
        
        # Duplicate check
        if duplicate_count == 0:
            response += "✅ **No duplicate entries** - each ward has unique data.\n\n"
        else:
            response += f"⚠️ Found {duplicate_count} duplicate rows that may need review.\n\n"
        
        # Extract key statistics dynamically from the output
        num_rows = None
        num_cols = None
        num_numeric = 0
        num_categorical = 0
        
        # Parse the actual data from output
        for line in lines:
            if 'rows' in line and 'columns' in line:
                # Try to extract from summary line
                import re
                numbers = re.findall(r'\d+', line)
                if len(numbers) >= 2:
                    try:
                        num_rows = int(numbers[0])
                        num_cols = int(numbers[1])
                    except (ValueError, IndexError):
                        pass
            elif 'float64' in line:
                num_numeric += 1
            elif 'object' in line and 'dtype' not in line:
                num_categorical += 1
        
        response += "**Key Dataset Characteristics:**\n"
        if num_rows and num_cols:
            response += f"• {num_rows} geographic units across your study area\n"
            response += f"• {num_cols} variables covering environmental, health, and socioeconomic factors\n"
        if num_numeric > 0 or num_categorical > 0:
            response += f"• Both numeric indicators ({num_numeric}) and categorical identifiers ({num_categorical})\n\n"
        else:
            response += "• Mix of numeric and categorical variables\n\n"
        
        # Parse actual variables from output
        health_vars = []
        env_vars = []
        other_vars = []
        
        # Extract variable names from the output
        for line in lines:
            line_lower = line.lower()
            if any(term in line_lower for term in ['pfpr', 'malaria', 'parasite', 'tpr', 'rdt', 'case', 'incidence']):
                var_name = line.split()[0]
                if var_name not in ['dtype:', 'Missing', 'Number'] and not var_name.endswith(':'):
                    health_vars.append(var_name)
            elif any(term in line_lower for term in ['rain', 'temp', 'humid', 'water', 'evi', 'ndvi', 'soil']):
                var_name = line.split()[0]
                if var_name not in ['dtype:', 'Missing', 'Number'] and not var_name.endswith(':'):
                    env_vars.append(var_name)
            elif any(term in line_lower for term in ['housing', 'settlement', 'urban', 'building']):
                var_name = line.split()[0]
                if var_name not in ['dtype:', 'Missing', 'Number'] and not var_name.endswith(':'):
                    other_vars.append(var_name)
        
        if health_vars or env_vars or other_vars:
            response += "**Malaria-Relevant Variables Found:**\n"
            if health_vars:
                response += f"• **Health indicators**: {', '.join(health_vars[:3])}\n"
            if env_vars:
                response += f"• **Environmental factors**: {', '.join(env_vars[:4])}\n"
            if other_vars:
                response += f"• **Risk modifiers**: {', '.join(other_vars[:3])}\n"
            response += "\n"
        
        # Readiness assessment
        response += "**Analysis Readiness: ✅ Ready**\n"
        if missing_count < 50:
            response += "Your data quality is excellent for proceeding with malaria risk analysis. "
        else:
            response += "Your data is suitable for analysis, though some variables have missing values. "
        response += "You can now run comprehensive malaria risk assessment to identify priority wards for intervention.\n\n"
        
        response += "Would you like me to:\n"
        response += "• Run the full malaria risk analysis?\n"
        response += "• Explore specific variables in detail?\n"
        response += "• Create visualizations of key indicators?"
        
        return response
    
    def _format_general_output(self, output: str, context: str) -> str:
        """Format general analysis output."""
        # Return clean output without extra headers
        return output.strip()
    
    def _format_column_listing(self, df: pd.DataFrame) -> str:
        """Format column listing when user asks for variables in dataset."""
        columns = df.columns.tolist()
        
        # Group columns by category
        health_vars = []
        env_vars = []
        socio_vars = []
        geo_vars = []
        other_vars = []
        
        for col in columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ['pfpr', 'malaria', 'parasite', 'tpr', 'rdt', 'case', 'health']):
                health_vars.append(col)
            elif any(term in col_lower for term in ['rain', 'temp', 'humid', 'water', 'evi', 'ndvi', 'soil', 'flood', 'elevation']):
                env_vars.append(col)
            elif any(term in col_lower for term in ['housing', 'settlement', 'urban', 'building', 'population']):
                socio_vars.append(col)
            elif any(term in col_lower for term in ['ward', 'state', 'lga', 'code', 'id', 'name']):
                geo_vars.append(col)
            else:
                other_vars.append(col)
        
        # Build response
        response = f"**Your dataset contains {len(columns)} variables:**\n\n"
        
        if health_vars:
            response += "**Health Indicators:**\n"
            for var in health_vars:
                response += f"• {var}\n"
            response += "\n"
        
        if env_vars:
            response += "**Environmental Factors:**\n"
            for var in env_vars:
                response += f"• {var}\n"
            response += "\n"
        
        if socio_vars:
            response += "**Socioeconomic Variables:**\n"
            for var in socio_vars:
                response += f"• {var}\n"
            response += "\n"
        
        if geo_vars:
            response += "**Geographic Identifiers:**\n"
            for var in geo_vars:
                response += f"• {var}\n"
            response += "\n"
        
        if other_vars:
            response += "**Other Variables:**\n"
            for var in other_vars:
                response += f"• {var}\n"
            response += "\n"
        
        response += "You can explore any of these variables, create maps, or run analysis. What would you like to know?"
        
        return response


def get_conversational_data_access(session_id: str, llm_manager=None) -> ConversationalDataAccess:
    """Factory function to create conversational data access."""
    return ConversationalDataAccess(session_id, llm_manager)