"""
Centralized Variable Resolution Service for ChatMRPT

This service provides consistent variable matching across the entire system,
replacing the inconsistent direct column matching used in 33+ files.

Key features:
- Fuzzy matching for all variable names
- Caching for performance
- Suggestions when no match found
- Works with any dataset (not hardcoded for malaria)
"""

import logging
from typing import List, Dict, Optional, Set, Tuple, Any
from functools import lru_cache
from difflib import get_close_matches
from datetime import datetime

from app.core.variable_matcher import VariableMatcher

logger = logging.getLogger(__name__)


class VariableResolutionService:
    """
    Centralized service for resolving variable names across ChatMRPT.
    
    This replaces all the `if var in columns` checks scattered throughout
    the codebase with intelligent fuzzy matching.
    """
    
    def __init__(self):
        self.matcher = VariableMatcher()
        self._cache = {}
        self._column_cache = {}
        self._dataset_fingerprints = {}  # Cache for dataset fingerprinting
        self._batch_cache = {}  # Cache for batch operations
        
    def resolve_variable(self, 
                        user_variable: str, 
                        available_columns: List[str],
                        threshold: float = 0.7,
                        return_suggestions: bool = True) -> Dict[str, Any]:
        """
        Resolve a single variable name to the best matching column.
        
        Args:
            user_variable: The variable name provided by the user
            available_columns: List of actual column names in the dataset
            threshold: Minimum similarity score (0-1) to accept a match
            return_suggestions: Whether to return alternative suggestions
            
        Returns:
            Dict with:
            - matched: The matched column name (or None)
            - confidence: Confidence score (0-1)
            - suggestions: List of alternative suggestions (if requested)
            - original: The original user variable name
        """
        # Performance optimization: Use dataset fingerprint for consistent caching
        dataset_fingerprint = self.get_dataset_fingerprint(available_columns)
        cache_key = f"{user_variable}:{dataset_fingerprint}:{threshold}"
        
        if cache_key in self._cache:
            logger.debug(f"Cache hit for variable '{user_variable}'")
            return self._cache[cache_key]
        
        # Use cached find_best_match for performance
        columns_tuple = tuple(sorted(available_columns))
        result = self._cached_find_best_match(user_variable, columns_tuple, threshold)
        
        # Format the response
        response = {
            'matched': result['matched_variable'],
            'confidence': result['confidence'],
            'original': user_variable,
            'suggestions': result.get('suggestions', []) if return_suggestions else []
        }
        
        # Log the resolution
        if response['matched']:
            if response['confidence'] < 1.0:
                logger.info(f"Fuzzy matched '{user_variable}' → '{response['matched']}' "
                          f"(confidence: {response['confidence']:.0%})")
            else:
                logger.debug(f"Exact match: '{user_variable}' → '{response['matched']}'")
        else:
            logger.warning(f"No match found for '{user_variable}'. "
                         f"Best candidates: {response['suggestions'][:3]}")
        
        # Cache the result
        self._cache[cache_key] = response
        return response
    
    def resolve_multiple_variables(self,
                                 user_variables: List[str],
                                 available_columns: List[str],
                                 threshold: float = 0.7,
                                 stop_on_error: bool = False) -> Dict[str, Any]:
        """
        Resolve multiple variables at once using batch processing for performance.
        
        Args:
            user_variables: List of variable names from user
            available_columns: List of actual column names
            threshold: Minimum similarity score
            stop_on_error: If True, stop on first unmatched variable
            
        Returns:
            Dict with:
            - matched: Dict mapping user vars to matched columns
            - unmatched: List of variables that couldn't be matched
            - suggestions: Dict of suggestions for unmatched vars
            - all_matched: Boolean indicating if all variables matched
        """
        # Use batch processing for better performance
        if not stop_on_error and len(user_variables) > 3:
            # For larger batches without stop_on_error, use batch processing
            batch_results = self.resolve_variables_batch(user_variables, available_columns, threshold)
            
            matched = {}
            unmatched = []
            suggestions = {}
            
            for var, result in batch_results.items():
                if result['matched']:
                    matched[var] = result['matched']
                else:
                    unmatched.append(var)
                    if result['suggestions']:
                        suggestions[var] = result['suggestions']
            
            return {
                'matched': matched,
                'unmatched': unmatched,
                'suggestions': suggestions,
                'all_matched': len(unmatched) == 0
            }
        
        # For smaller batches or with stop_on_error, use sequential processing
        matched = {}
        unmatched = []
        suggestions = {}
        
        for var in user_variables:
            result = self.resolve_variable(
                var, 
                available_columns, 
                threshold, 
                return_suggestions=True
            )
            
            if result['matched']:
                matched[var] = result['matched']
            else:
                unmatched.append(var)
                if result['suggestions']:
                    suggestions[var] = result['suggestions']
                
                if stop_on_error:
                    break
        
        return {
            'matched': matched,
            'unmatched': unmatched,
            'suggestions': suggestions,
            'all_matched': len(unmatched) == 0
        }
    
    def check_column_exists(self, 
                          column: str, 
                          available_columns: List[str],
                          auto_resolve: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Check if a column exists, with optional auto-resolution.
        
        This is a drop-in replacement for `if column in columns` checks.
        
        Args:
            column: Column name to check
            available_columns: List of available columns
            auto_resolve: If True, try to fuzzy match if exact match fails
            
        Returns:
            Tuple of (exists, resolved_name)
        """
        # First try exact match (case-sensitive)
        if column in available_columns:
            return True, column
        
        # Try case-insensitive match
        for col in available_columns:
            if column.lower() == col.lower():
                return True, col
        
        # If auto_resolve is enabled, try fuzzy matching
        if auto_resolve:
            result = self.resolve_variable(column, available_columns)
            if result['matched'] and result['confidence'] >= 0.8:
                return True, result['matched']
        
        return False, None
    
    def get_numeric_columns(self, 
                          df,
                          exclude_patterns: List[str] = None) -> List[str]:
        """
        Get all numeric columns from a dataframe, with intelligent filtering.
        
        Args:
            df: Pandas DataFrame
            exclude_patterns: Patterns to exclude (e.g., ['id', 'code'])
            
        Returns:
            List of numeric column names
        """
        if exclude_patterns is None:
            exclude_patterns = [
                'id', 'code', 'name', 'index', 'x', 'timestamp', 
                'date', 'geometry', 'shape'
            ]
        
        numeric_cols = []
        for col in df.select_dtypes(include=['int64', 'float64']).columns:
            # Check if column should be excluded
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in exclude_patterns):
                continue
            numeric_cols.append(col)
        
        return numeric_cols
    
    def suggest_similar_variables(self,
                                user_input: str,
                                available_columns: List[str],
                                max_suggestions: int = 5) -> List[str]:
        """
        Get suggestions for similar variable names.
        
        Args:
            user_input: The user's input
            available_columns: Available column names
            max_suggestions: Maximum number of suggestions
            
        Returns:
            List of suggested column names
        """
        # Use both our matcher and difflib for comprehensive suggestions
        matcher_result = self.resolve_variable(
            user_input, 
            available_columns,
            threshold=0.5,
            return_suggestions=True
        )
        
        # Also use difflib for additional suggestions
        difflib_matches = get_close_matches(
            user_input, 
            available_columns, 
            n=max_suggestions,
            cutoff=0.4
        )
        
        # Combine and deduplicate
        all_suggestions = []
        seen = set()
        
        # Add matcher suggestions first (they're usually better)
        for sug in matcher_result['suggestions']:
            if sug not in seen:
                all_suggestions.append(sug)
                seen.add(sug)
        
        # Add difflib suggestions
        for sug in difflib_matches:
            if sug not in seen and len(all_suggestions) < max_suggestions:
                all_suggestions.append(sug)
                seen.add(sug)
        
        return all_suggestions[:max_suggestions]
    
    def create_variable_error_message(self,
                                    user_variable: str,
                                    available_columns: List[str],
                                    context: str = "") -> str:
        """
        Create a helpful error message when a variable isn't found.
        
        Args:
            user_variable: The variable that wasn't found
            available_columns: Available columns
            context: Additional context for the error
            
        Returns:
            Formatted error message with suggestions
        """
        suggestions = self.suggest_similar_variables(user_variable, available_columns)
        
        msg = f"Variable '{user_variable}' not found"
        if context:
            msg += f" {context}"
        msg += "."
        
        if suggestions:
            msg += f"\n\nDid you mean one of these?\n"
            for i, sug in enumerate(suggestions, 1):
                msg += f"  {i}. {sug}\n"
        
        # Show some available variables if no good suggestions
        if not suggestions and available_columns:
            numeric_cols = self.get_numeric_columns_from_list(available_columns)
            if numeric_cols:
                msg += f"\n\nAvailable analysis variables include:\n"
                for col in numeric_cols[:10]:
                    msg += f"  • {col}\n"
                if len(numeric_cols) > 10:
                    msg += f"  ... and {len(numeric_cols) - 10} more\n"
        
        return msg
    
    def get_numeric_columns_from_list(self, columns: List[str]) -> List[str]:
        """Helper to filter likely numeric columns from a column list."""
        exclude_patterns = [
            'id', 'code', 'name', 'index', 'x', 'timestamp', 
            'date', 'geometry', 'shape', 'ward', 'lga', 'state'
        ]
        
        numeric_cols = []
        for col in columns:
            col_lower = col.lower()
            # Skip if it matches exclude patterns
            if any(pattern in col_lower for pattern in exclude_patterns):
                continue
            # Include if it doesn't look like an identifier
            if not col_lower.startswith(('x', 'unnamed')):
                numeric_cols.append(col)
        
        return numeric_cols
    
    def clear_cache(self):
        """Clear the resolution cache."""
        self._cache.clear()
        self._column_cache.clear()
        self._dataset_fingerprints.clear()
        self._batch_cache.clear()
        logger.info("Variable resolution cache cleared")
    
    def _create_dataset_fingerprint(self, columns: List[str]) -> str:
        """Create a unique fingerprint for a dataset based on its columns."""
        # Sort columns for consistent fingerprinting
        sorted_columns = sorted(columns)
        # Create hash of column names
        import hashlib
        fingerprint = hashlib.md5('|'.join(sorted_columns).encode()).hexdigest()
        return fingerprint
    
    def get_dataset_fingerprint(self, columns: List[str]) -> str:
        """Get or create a dataset fingerprint for caching purposes."""
        fingerprint = self._create_dataset_fingerprint(columns)
        self._dataset_fingerprints[fingerprint] = {
            'columns': columns,
            'timestamp': datetime.now().isoformat()
        }
        return fingerprint
    
    @lru_cache(maxsize=500)
    def _cached_find_best_match(self, user_variable: str, columns_tuple: tuple, threshold: float):
        """Cached version of find_best_match for performance."""
        columns_list = list(columns_tuple)
        return self.matcher._find_best_match(user_variable, columns_list, threshold)
    
    def resolve_variables_batch(self, 
                               user_variables: List[str],
                               available_columns: List[str],
                               threshold: float = 0.7) -> Dict[str, Dict[str, Any]]:
        """
        Efficiently resolve multiple variables using batch processing and caching.
        
        Args:
            user_variables: List of user variable names
            available_columns: Available column names
            threshold: Matching threshold
            
        Returns:
            Dict mapping user variables to resolution results
        """
        # Create batch cache key
        columns_tuple = tuple(sorted(available_columns))
        variables_tuple = tuple(sorted(user_variables))
        batch_key = f"{hash(variables_tuple)}:{hash(columns_tuple)}:{threshold}"
        
        # Check batch cache
        if batch_key in self._batch_cache:
            logger.debug(f"Batch cache hit for {len(user_variables)} variables")
            return self._batch_cache[batch_key]
        
        # Process in batch
        results = {}
        for var in user_variables:
            # Use cached find_best_match for performance
            match_result = self._cached_find_best_match(var, columns_tuple, threshold)
            
            results[var] = {
                'matched': match_result['matched_variable'],
                'confidence': match_result['confidence'],
                'original': var,
                'suggestions': match_result.get('suggestions', [])
            }
        
        # Cache the batch result
        self._batch_cache[batch_key] = results
        
        return results


# Global instance for easy access
variable_resolver = VariableResolutionService()


# Convenience functions for backward compatibility
def resolve_variable(user_variable: str, 
                    available_columns: List[str],
                    **kwargs) -> Dict[str, Any]:
    """Resolve a single variable using the global resolver."""
    return variable_resolver.resolve_variable(user_variable, available_columns, **kwargs)


def check_column_exists(column: str,
                       available_columns: List[str],
                       auto_resolve: bool = True) -> Tuple[bool, Optional[str]]:
    """Check if a column exists with optional fuzzy matching."""
    return variable_resolver.check_column_exists(column, available_columns, auto_resolve)


def create_variable_error_message(user_variable: str,
                                available_columns: List[str],
                                context: str = "") -> str:
    """Create a helpful error message with suggestions."""
    return variable_resolver.create_variable_error_message(
        user_variable, available_columns, context
    )