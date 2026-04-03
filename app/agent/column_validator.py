"""
Column Validator for Data Analysis V3
Validates and fixes column references in generated code
"""

import re
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class ColumnValidator:
    """
    Validates and fixes column references in Python code before execution.
    Maps common incorrect column names to actual column names.
    """
    
    # Common column name mappings
    COLUMN_MAPPINGS = {
        # Ward mappings
        'ward': 'WardName',
        'Ward': 'WardName',
        'ward_name': 'WardName',
        'wards': 'WardName',
        
        # LGA mappings
        'lga': 'LGA',
        'lga_name': 'LGA',
        'local_government': 'LGA',
        'local_govt': 'LGA',
        
        # State mappings
        'state': 'State',
        'state_name': 'State',
        
        # Facility mappings
        'facility': 'HealthFacility',
        'facility_id': 'HealthFacility',
        'health_facility': 'HealthFacility',
        'facility_name': 'HealthFacility',
        
        # Facility level mappings
        'facility_type': 'FacilityLevel',
        'facility_level': 'FacilityLevel',
        
        # Age group mappings (for specific columns)
        'age_group': None,  # Will be handled specially
        'age': None,  # Will be handled specially
    }
    
    @classmethod
    def validate_and_fix_code(cls, code: str, actual_columns: List[str]) -> str:
        """
        Validate and fix column references in code.
        
        Args:
            code: Python code to validate
            actual_columns: List of actual column names in the data
            
        Returns:
            Fixed code with correct column names
        """
        fixed_code = code
        issues_found = []
        
        # Create a mapping of lowercase to actual columns for case-insensitive matching
        actual_columns_lower = {col.lower(): col for col in actual_columns}
        
        # Pattern to find DataFrame column references
        # Matches: df['column'], df["column"], df.column
        patterns = [
            (r"df\['([^']+)'\]", "df['{}']"),  # Single quotes
            (r'df\["([^"]+)"\]', 'df["{}"]'),  # Double quotes
            (r"df\.([a-zA-Z_][a-zA-Z0-9_]*)", "df.{}"),  # Dot notation
        ]
        
        for pattern, replacement_format in patterns:
            matches = re.finditer(pattern, fixed_code)
            replacements = []
            
            for match in matches:
                column_ref = match.group(1)
                
                # Skip if it's a method call (like df.head(), df.groupby())
                if column_ref in ['head', 'tail', 'groupby', 'describe', 'info', 'shape', 
                                  'columns', 'index', 'values', 'dtypes', 'nunique', 
                                  'isnull', 'notnull', 'mean', 'sum', 'count', 'std',
                                  'min', 'max', 'median', 'mode', 'loc', 'iloc']:
                    continue
                
                # Check if column exists (case-insensitive)
                if column_ref not in actual_columns:
                    # Try to find the correct column
                    correct_column = cls._find_correct_column(column_ref, actual_columns, actual_columns_lower)
                    
                    if correct_column:
                        # Store replacement
                        old_ref = match.group(0)
                        new_ref = replacement_format.format(correct_column)
                        replacements.append((old_ref, new_ref))
                        issues_found.append(f"Fixed: '{column_ref}' -> '{correct_column}'")
                    else:
                        # Column not found - this will likely cause an error
                        logger.warning(f"Column '{column_ref}' not found in actual columns")
                        issues_found.append(f"Warning: Column '{column_ref}' not found")
            
            # Apply replacements in reverse order to maintain correct positions
            for old, new in reversed(replacements):
                fixed_code = fixed_code.replace(old, new)
        
        # Special handling for age_group references
        if 'age_group' in fixed_code.lower():
            # Check if data has specific age columns
            age_columns = [col for col in actual_columns if any(
                age_indicator in col.lower() 
                for age_indicator in ['u5', '<5', 'o5', '≥5', 'pw', 'preg', 'all']
            )]
            
            if age_columns:
                # Log a warning about age_group reference
                logger.info(f"Found age-related columns: {age_columns}")
                issues_found.append(f"Note: Found age columns: {', '.join(age_columns[:3])}")
        
        if issues_found:
            logger.info(f"Column validation issues: {'; '.join(issues_found)}")
        
        return fixed_code
    
    @classmethod
    def _find_correct_column(cls, column_ref: str, actual_columns: List[str], 
                            actual_columns_lower: Dict[str, str]) -> Optional[str]:
        """
        Find the correct column name for a reference.
        
        Args:
            column_ref: The column reference to fix
            actual_columns: List of actual column names
            actual_columns_lower: Lowercase mapping of actual columns
            
        Returns:
            Correct column name or None if not found
        """
        # First check direct mapping
        if column_ref in cls.COLUMN_MAPPINGS:
            mapped = cls.COLUMN_MAPPINGS[column_ref]
            if mapped and mapped in actual_columns:
                return mapped
        
        # Check case-insensitive match
        column_lower = column_ref.lower()
        if column_lower in actual_columns_lower:
            return actual_columns_lower[column_lower]
        
        # Try fuzzy matching for common variations
        # Check if it's a partial match (e.g., 'ward' in 'WardName')
        for actual_col in actual_columns:
            if column_lower in actual_col.lower() or actual_col.lower() in column_lower:
                # Prefer exact word matches
                if column_lower == actual_col.lower().split('_')[0] or \
                   column_lower == actual_col.lower().split()[-1]:
                    return actual_col
        
        # Check common prefixes/suffixes
        for actual_col in actual_columns:
            actual_lower = actual_col.lower()
            # Remove common prefixes/suffixes and compare
            clean_ref = column_lower.replace('_name', '').replace('_id', '')
            clean_actual = actual_lower.replace('name', '').replace('id', '')
            if clean_ref == clean_actual:
                return actual_col
        
        return None
    
    @classmethod
    def get_column_summary(cls, columns: List[str]) -> str:
        """
        Generate a clear summary of available columns for the LLM.
        
        Args:
            columns: List of column names
            
        Returns:
            Formatted column summary
        """
        summary = "EXACT column names in the data:\n"
        
        # Group columns by category
        geographic = []
        test_data = []
        facility = []
        other = []
        
        for col in columns:
            col_lower = col.lower()
            if any(geo in col_lower for geo in ['ward', 'lga', 'state']):
                geographic.append(col)
            elif any(test in col_lower for test in ['rdt', 'microscopy', 'tested', 'positive']):
                test_data.append(col)
            elif any(fac in col_lower for fac in ['facility', 'health']):
                facility.append(col)
            else:
                other.append(col)
        
        if geographic:
            quoted = [f"'{c}'" for c in geographic[:5]]
            summary += f"\nGeographic columns: {', '.join(quoted)}"
        if test_data:
            quoted = [f"'{c}'" for c in test_data[:10]]
            summary += f"\nTest data columns: {', '.join(quoted)}"
        if facility:
            quoted = [f"'{c}'" for c in facility[:5]]
            summary += f"\nFacility columns: {', '.join(quoted)}"
        if other and len(other) <= 10:
            quoted = [f"'{c}'" for c in other]
            summary += f"\nOther columns: {', '.join(quoted)}"
        
        summary += f"\n\nTotal: {len(columns)} columns"
        summary += "\n\nIMPORTANT: Use these EXACT column names in your code!"
        
        return summary