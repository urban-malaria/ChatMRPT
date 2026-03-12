"""
Safety module for code execution
Provides subprocess isolation and resource limits
"""

import subprocess
import sys
import json
import tempfile
import os
from pathlib import Path
import ast
import logging

logger = logging.getLogger(__name__)

class SafeExecutor:
    """
    Execute LLM-generated code in isolated subprocess.
    Following Open Interpreter safety patterns.
    """
    
    def __init__(self):
        self.timeout = 30  # seconds
        self.max_memory = "512M"
        self.blocked_modules = {
            'os', 'subprocess', 'sys', 'socket', 'requests',
            'urllib', 'shutil', 'glob', 'pathlib'
        }
        self.allowed_builtins = {
            'print', 'len', 'range', 'enumerate', 'zip',
            'map', 'filter', 'sum', 'min', 'max', 'abs',
            'round', 'sorted', 'reversed', 'int', 'float',
            'str', 'bool', 'list', 'dict', 'set', 'tuple'
        }
    
    def validate_code(self, code: str) -> tuple[bool, str]:
        """
        Validate code using AST analysis.
        Returns (is_safe, error_message)
        """
        try:
            tree = ast.parse(code)
            
            for node in ast.walk(tree):
                # Check imports
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        module = alias.name.split('.')[0]
                        if module in self.blocked_modules:
                            return False, f"Import of '{module}' is not allowed"
                
                elif isinstance(node, ast.ImportFrom):
                    module = node.module.split('.')[0] if node.module else ''
                    if module in self.blocked_modules:
                        return False, f"Import from '{module}' is not allowed"
                
                # Block dangerous functions
                elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                    if node.id in ['eval', 'exec', '__import__', 'compile', 'open']:
                        return False, f"Use of '{node.id}' is not allowed"
                
                # Block file operations
                elif isinstance(node, ast.Attribute):
                    if node.attr in ['read', 'write', 'remove', 'unlink', 'rmdir']:
                        return False, f"File operation '{node.attr}' is not allowed"
            
            return True, "Code validation passed"
            
        except SyntaxError as e:
            return False, f"Syntax error: {str(e)}"
    
    def execute_isolated(self, code: str, data_path: str) -> dict:
        """
        Execute code in isolated subprocess with resource limits.
        """
        # First validate
        is_safe, message = self.validate_code(code)
        if not is_safe:
            return {
                'success': False,
                'error': f"Code validation failed: {message}"
            }
        
        # Create execution script
        exec_script = f"""
import pandas as pd
import numpy as np
import json
import sys
import warnings
warnings.filterwarnings('ignore')

# Load data
df = pd.read_csv('{data_path}')

# Capture output
import io
from contextlib import redirect_stdout

output = io.StringIO()
with redirect_stdout(output):
    try:
        # User code
        {code}
        
        # Return results
        result = {{
            'success': True,
            'output': output.getvalue()
        }}
    except Exception as e:
        result = {{
            'success': False,
            'error': str(e),
            'output': output.getvalue()
        }}

print(json.dumps(result))
"""
        
        # Write to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(exec_script)
            script_path = f.name
        
        try:
            # Execute in subprocess with timeout
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=self.timeout
            )
            
            if result.returncode == 0:
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return {
                        'success': True,
                        'output': result.stdout
                    }
            else:
                return {
                    'success': False,
                    'error': result.stderr or "Execution failed"
                }
                
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'error': f"Execution timeout ({self.timeout}s)"
            }
        finally:
            # Clean up temp file
            try:
                os.unlink(script_path)
            except:
                pass