"""
Data Analysis Module - Separate LLM-powered analysis pipeline
Completely independent from main risk analysis flow
Can be tested and developed without affecting existing functionality
"""

from .executor import DataExecutor
from .prompts import AnalysisPrompts

__all__ = ['DataExecutor', 'AnalysisPrompts']

# Module metadata
__version__ = '0.1.0'
__status__ = 'experimental'
__description__ = 'LLM-powered data analysis with full code execution capabilities'