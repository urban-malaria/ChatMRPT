"""
Data Query Tool for ChatMRPT
Enables direct pandas/numpy code execution on session data

This tool provides unlimited data exploration capabilities by allowing
the LLM to execute pandas operations directly on the dataset.
"""

import logging
from typing import Dict, Any, List
from pydantic import Field, validator
import pandas as pd
import numpy as np

from .base import BaseTool, ToolExecutionResult, ToolCategory
from ..services.conversational_data_access import get_conversational_data_access

logger = logging.getLogger(__name__)


class ExecuteDataQuery(BaseTool):
    """
    Execute pandas/numpy code on session data for unlimited exploration.
    
    This tool enables conversational data analysis by allowing direct
    code execution on the dataset. It provides safe execution environment
    with comprehensive data access.
    """
    
    code: str = Field(
        description="Python code to execute on the dataset (pandas/numpy operations)",
        min_length=1
    )
    
    context: str = Field(
        default="",
        description="Context or explanation of what the code should do"
    )
    
    @validator('code')
    def validate_code(cls, v):
        """Validate that code is safe and reasonable."""
        if not v.strip():
            raise ValueError("Code cannot be empty")
        
        # Use the same validation logic as the conversational data access
        if not cls._is_code_safe(v):
            raise ValueError("Code contains potentially dangerous operations")
        
        return v
    
    @classmethod
    def _is_code_safe(cls, code: str) -> bool:
        """Check if code is safe to execute."""
        import re
        
        # Convert to lowercase for case-insensitive checking
        code_lower = code.lower()
        
        # Comprehensive list of dangerous patterns
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
                return False
        
        # Check for dangerous imports using regex
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
                return False
        
        # Check for attribute access to dangerous modules
        dangerous_attrs = [
            r'\.os\.',
            r'\.sys\.',
            r'\.subprocess\.',
            r'\.socket\.',
        ]
        
        for pattern in dangerous_attrs:
            if re.search(pattern, code_lower):
                return False
        
        return True
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show correlation between rainfall and malaria cases",
            "Display top 10 wards by population",
            "Create scatter plot of composite vs PCA scores",
            "Calculate mean ITN coverage by region",
            "Find wards with missing healthcare access data",
            "Show statistical summary of all numeric variables",
            "Group wards by risk category and show averages",
            "Create heatmap of variable correlations"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute pandas/numpy code on session data."""
        try:
            # Get conversational data access
            data_access = get_conversational_data_access(session_id)
            
            # Check if data is available
            df, info = data_access.get_available_data()
            if df is None:
                return self._create_error_result(
                    f"No data available for query: {info.get('error', 'Unknown error')}"
                )
            
            # Execute the code
            result = data_access.execute_code(self.code, self.context)
            
            if result['success']:
                # Format successful result
                response_parts = []
                
                # Add context if provided
                if self.context:
                    response_parts.append(f"**Query**: {self.context}")
                
                # Add code executed
                response_parts.append(f"**Code executed**:")
                response_parts.append(f"```python\n{self.code}\n```")
                
                # Add output
                if result['output']:
                    response_parts.append(f"**Output**:")
                    response_parts.append(f"```\n{result['output']}\n```")
                
                # Add any errors/warnings
                if result['error']:
                    response_parts.append(f"**Warnings**:")
                    response_parts.append(f"```\n{result['error']}\n```")
                
                # Handle plot data
                plot_info = {}
                if result['plot_data']:
                    plot_info = {
                        'plot_generated': True,
                        'plot_data': result['plot_data']
                    }
                    response_parts.append("**Visualization**: Plot generated successfully")
                
                # Add variables created
                if result['variables_created']:
                    response_parts.append(f"**Variables created**: {', '.join(result['variables_created'])}")
                
                message = "\n\n".join(response_parts)
                
                return self._create_success_result(
                    message=message,
                    data={
                        'code_executed': self.code,
                        'context': self.context,
                        'output': result['output'],
                        'execution_success': True,
                        'data_stage': info.get('stage', 'unknown'),
                        'variables_created': result['variables_created'],
                        **plot_info
                    }
                )
            
            else:
                # Handle execution error
                error_parts = []
                error_parts.append(f"**Code execution failed**:")
                error_parts.append(f"```python\n{self.code}\n```")
                error_parts.append(f"**Error**: {result['error']}")
                
                if result.get('traceback'):
                    error_parts.append(f"**Traceback**:")
                    error_parts.append(f"```\n{result['traceback']}\n```")
                
                if result.get('output'):
                    error_parts.append(f"**Partial output**:")
                    error_parts.append(f"```\n{result['output']}\n```")
                
                message = "\n\n".join(error_parts)
                
                return self._create_error_result(
                    message,
                    data={
                        'code_executed': self.code,
                        'context': self.context,
                        'execution_success': False,
                        'error': result['error'],
                        'traceback': result.get('traceback', '')
                    }
                )
                
        except Exception as e:
            logger.error(f"Error in ExecuteDataQuery: {e}")
            return self._create_error_result(
                f"Data query execution failed: {str(e)}",
                data={
                    'code_executed': self.code,
                    'context': self.context,
                    'execution_success': False,
                    'error': str(e)
                }
            )


class ExploreDataSchema(BaseTool):
    """
    Explore the current dataset schema and structure.
    
    This tool provides comprehensive information about the available
    dataset including columns, data types, missing values, and 
    malaria-relevant variable analysis.
    """
    
    include_sample_data: bool = Field(
        default=True,
        description="Include sample data rows in the response"
    )
    
    include_correlations: bool = Field(
        default=False,
        description="Include correlation analysis for numeric variables"
    )
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Show me the dataset structure",
            "What columns are available?",
            "Explore data types and missing values",
            "Get comprehensive data schema",
            "Show sample data and correlations"
        ]
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Get comprehensive dataset schema information."""
        try:
            # Get conversational data access
            data_access = get_conversational_data_access(session_id)
            
            # Get comprehensive schema
            schema = data_access.generate_comprehensive_schema()
            
            if 'error' in schema:
                return self._create_error_result(
                    f"Cannot explore data schema: {schema['error']}"
                )
            
            # Format schema information
            response_parts = []
            
            # Dataset overview
            dataset_info = schema['dataset_info']
            response_parts.append(f"## Dataset Overview")
            response_parts.append(f"- **Stage**: {dataset_info['stage'].upper()}")
            response_parts.append(f"- **Rows**: {dataset_info['total_rows']:,}")
            response_parts.append(f"- **Columns**: {dataset_info['total_columns']}")
            response_parts.append(f"- **Memory Usage**: {dataset_info['memory_usage_mb']} MB")
            response_parts.append(f"- **Data Type**: {dataset_info['data_type']}")
            
            # Column analysis
            response_parts.append(f"\n## Column Analysis")
            
            # Group columns by malaria relevance
            columns_by_relevance = {}
            for col, info in schema['columns'].items():
                category = info['malaria_relevance']['category']
                if category not in columns_by_relevance:
                    columns_by_relevance[category] = []
                columns_by_relevance[category].append({
                    'name': col,
                    'type': info['category'],
                    'score': info['malaria_relevance']['score'],
                    'completeness': 100 - info['null_percentage']
                })
            
            # Display by relevance category
            category_order = ['health_direct', 'health_intervention', 'environmental', 
                            'analysis_results', 'socioeconomic', 'demographic', 
                            'geographic', 'unknown']
            
            for category in category_order:
                if category in columns_by_relevance:
                    cols = sorted(columns_by_relevance[category], key=lambda x: x['score'], reverse=True)
                    category_name = category.replace('_', ' ').title()
                    response_parts.append(f"\n### {category_name}")
                    
                    for col in cols:
                        response_parts.append(f"- **{col['name']}**: {col['type']} ({col['completeness']:.1f}% complete)")
            
            # Available operations
            response_parts.append(f"\n## Available Operations")
            for op in schema['available_operations'][:10]:  # Show first 10
                response_parts.append(f"- {op}")
            
            if len(schema['available_operations']) > 10:
                response_parts.append(f"- ... and {len(schema['available_operations']) - 10} more operations")
            
            # Sample data
            if self.include_sample_data and schema['sample_data']:
                response_parts.append(f"\n## Sample Data")
                response_parts.append("```python")
                
                # Format sample data nicely
                sample_df = pd.DataFrame(schema['sample_data'])
                if len(sample_df.columns) > 6:
                    # Show first 6 columns
                    sample_df = sample_df.iloc[:, :6]
                    truncated = True
                else:
                    truncated = False
                
                response_parts.append(sample_df.to_string(index=False))
                
                if truncated:
                    response_parts.append(f"... ({len(schema['columns']) - 6} more columns)")
                
                response_parts.append("```")
            
            # Malaria context
            malaria_context = schema['malaria_context']
            response_parts.append(f"\n## Malaria Analysis Context")
            response_parts.append(f"- **Domain**: {malaria_context['domain']}")
            response_parts.append(f"- **Use Case**: {malaria_context['use_case']}")
            response_parts.append(f"- **Geographic Focus**: {malaria_context['geographic_focus']}")
            
            # Variable categories
            var_categories = malaria_context['variable_categories']
            for category, variables in var_categories.items():
                if variables:
                    category_name = category.replace('_', ' ').title()
                    response_parts.append(f"- **{category_name}**: {len(variables)} variables")
            
            message = "\n".join(response_parts)
            
            return self._create_success_result(
                message=message,
                data={
                    'schema': schema,
                    'data_stage': dataset_info['stage'],
                    'total_rows': dataset_info['total_rows'],
                    'total_columns': dataset_info['total_columns'],
                    'malaria_relevant_columns': len([col for col, info in schema['columns'].items() 
                                                   if info['malaria_relevance']['score'] >= 6])
                }
            )
            
        except Exception as e:
            logger.error(f"Error in ExploreDataSchema: {e}")
            return self._create_error_result(f"Schema exploration failed: {str(e)}")


# Register tools
__all__ = [
    'ExecuteDataQuery',
    'ExploreDataSchema'
]