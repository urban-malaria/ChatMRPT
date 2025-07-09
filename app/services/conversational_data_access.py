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
        """Determine current analysis stage."""
        try:
            from flask import session
            
            if session.get('analysis_complete', False):
                return 'post_analysis'
            elif session.get('csv_loaded', False):
                return 'pre_analysis'
            else:
                return 'no_data'
        except:
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
            'df.corr()',
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
                return {
                    'success': False,
                    'error': info.get('error', 'No data available'),
                    'output': '',
                    'code': code
                }
            
            # Set up execution environment
            local_vars = {
                'df': df,
                'data': df,  # Alternative name
                'session_id': self.session_id,
                'stage': info['stage']
            }
            
            # Capture output
            output_buffer = io.StringIO()
            error_buffer = io.StringIO()
            
            # Execute code with output capture
            with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
                try:
                    # Execute the code
                    exec(code, self.safe_globals, local_vars)
                    
                    # Get output
                    output = output_buffer.getvalue()
                    error_output = error_buffer.getvalue()
                    
                    # Handle matplotlib plots
                    plot_data = None
                    if plt.get_fignums():  # Check if any figures exist
                        plot_data = self._capture_plot()
                    
                    return {
                        'success': True,
                        'output': output,
                        'error': error_output,
                        'plot_data': plot_data,
                        'code': code,
                        'context': context,
                        'variables_created': [k for k in local_vars.keys() if k not in ['df', 'data', 'session_id', 'stage']]
                    }
                    
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
df.corr()          # Correlation matrix
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
- Return only the code, no explanations

CODE:
"""
            
            # Generate code
            response = self.llm_manager.generate_response(
                messages=[{"role": "user", "content": code_prompt}],
                temperature=0.1,
                max_tokens=500
            )
            
            generated_code = response.get('content', '').strip()
            
            # Clean up code (remove markdown formatting if present)
            if generated_code.startswith('```'):
                lines = generated_code.split('\n')
                generated_code = '\n'.join(lines[1:-1])
            
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


def get_conversational_data_access(session_id: str, llm_manager=None) -> ConversationalDataAccess:
    """Factory function to create conversational data access."""
    return ConversationalDataAccess(session_id, llm_manager)