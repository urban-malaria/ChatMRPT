"""
TPR Data Analyzer

Analyzes TPR data to provide rich contextual information at each decision point.
Generates statistics for states, facilities, and age groups to help users make informed choices.
"""

import json
import logging
import os
import re

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)

# Semantic fields the schema inference resolves
_SCHEMA_FIELDS = ('state', 'lga', 'ward', 'facility_name', 'facility_level', 'period')

# Keyword fallback lists per field (last resort when LLM unavailable)
_KEYWORD_MAP: Dict[str, List[str]] = {
    'state':          ['state', 'region', 'province'],
    'lga':            ['lga', 'local government'],
    'facility_name':  ['facility', 'clinic', 'hospital', 'health', 'center'],
    'facility_level': ['facilitylevel', 'facility level', 'facility_level',
                       'facilitytype', 'level', 'type', 'tier', 'category'],
    'ward':           ['ward'],
    'period':         ['period', 'periodname', 'date', 'month', 'year'],
}


class TPRDataAnalyzer:
    """
    Analyzes TPR data to generate contextual statistics for workflow decisions.
    
    Features:
    - State-level analysis with facility counts
    - Facility type distribution
    - Age group test availability and positivity rates
    - Data quality assessment
    - Recommendations based on data patterns
    """
    
    def __init__(self):
        """Initialize the TPR data analyzer."""
        self.data = None
        self.analysis_cache = {}
        self._schema: Optional[Dict[str, Optional[str]]] = None  # inferred once per instance
        
    def analyze_states(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze available states in the data.
        
        Args:
            df: DataFrame with TPR data
            
        Returns:
            Dictionary with state-level statistics
        """
        try:
            states_info = {}

            # Infer schema once (no-op on subsequent calls within this workflow run)
            self.ensure_schema(df)

            state_col = self._get_column(df, 'state')
            facility_col = self._get_column(df, 'facility_name')

            if not state_col:
                logger.warning("No state column detected in dataset")
                return {
                    'states': {},
                    'total_states': 0,
                    'recommended': None,
                    'state_column': None,
                    'state_column_detected': False,
                    'error': 'STATE_COLUMN_NOT_FOUND'
                }
            
            # Find test columns dynamically
            test_cols = self._find_test_columns(df)
            
            # Analyze each state
            for state in df[state_col].dropna().unique():
                state_data = df[df[state_col] == state]
                
                state_info = {
                    'name': str(state),
                    'display_name': self._strip_dhis2_prefix(state),
                    'total_records': len(state_data),
                    'facilities': len(state_data) if not facility_col else state_data[facility_col].nunique(),
                    'total_tests': self._count_total_tests(state_data),
                    'data_completeness': self._calculate_completeness(state_data)
                }
                
                states_info[str(state)] = state_info
            
            # Sort states by total tests (most data first)
            sorted_states = sorted(states_info.items(), 
                                 key=lambda x: x[1]['total_tests'], 
                                 reverse=True)
            
            return {
                'states': dict(sorted_states),
                'total_states': len(states_info),
                'recommended': sorted_states[0][0] if sorted_states else None,
                'state_column': state_col,
                'state_column_detected': True
            }
            
        except Exception as e:
            logger.error(f"Error analyzing states: {e}")
            return {'error': str(e), 'states': {}}
    
    def analyze_facility_levels(self, df: pd.DataFrame, state: str) -> Dict[str, Any]:
        """
        Analyze facility level distribution for a state.
        
        Args:
            df: DataFrame with TPR data
            state: Selected state name
            
        Returns:
            Dictionary with facility level statistics
        """
        try:
            # Schema should already be set from analyze_states; ensure it is
            self.ensure_schema(df)

            state_col = self._get_column(df, 'state')
            if state_col and state in df[state_col].values:
                df = df[df[state_col] == state]

            level_col = self._get_column(df, 'facility_level')
            
            if not level_col:
                # If no facility level column, return single "all" option
                return {
                    'levels': {
                        'all': {
                            'name': 'All Facilities',
                            'count': len(df),
                            'percentage': 100,
                            'description': 'All healthcare facilities',
                            'recommended': False
                        }
                    },
                    'has_levels': False
                }
            
            levels_info = {}
            total_facilities = len(df)
            
            # Analyze each level
            for level in df[level_col].dropna().unique():
                level_data = df[df[level_col] == level]
                
                # Normalize level name
                level_key = level.lower().replace(' ', '_')
                
                # Calculate test type stats
                rdt_tests = 0
                microscopy_tests = 0
                for col in level_data.columns:
                    col_lower = col.lower()
                    if pd.api.types.is_numeric_dtype(level_data[col]):
                        if 'rdt' in col_lower and ('test' in col_lower or 'examined' in col_lower):
                            if 'positive' not in col_lower and 'negative' not in col_lower:
                                rdt_tests += level_data[col].fillna(0).sum()
                        elif 'microscopy' in col_lower and ('test' in col_lower or 'examined' in col_lower):
                            if 'positive' not in col_lower and 'negative' not in col_lower:
                                microscopy_tests += level_data[col].fillna(0).sum()
                
                # Determine urban/rural split if possible
                urban_rural = self._analyze_urban_rural(level_data)
                
                levels_info[level_key] = {
                    'name': level,
                    'count': len(level_data),
                    'percentage': round((len(level_data) / total_facilities) * 100, 1),
                    'urban_percentage': urban_rural.get('urban', 0),
                    'rural_percentage': urban_rural.get('rural', 0),
                    'description': self._get_facility_description(level),
                    'rdt_tests': int(rdt_tests),
                    'microscopy_tests': int(microscopy_tests)
                }
            
            # Determine which level is Primary and mark it as recommended
            primary_found = False
            for key, info in levels_info.items():
                if 'primary' in info['name'].lower():
                    info['recommended'] = True
                    primary_found = True
                else:
                    info['recommended'] = False
            
            # Add "all" option (not recommended by default)
            levels_info['all'] = {
                'name': 'All Facilities',
                'count': total_facilities,
                'percentage': 100,
                'description': 'Complete coverage across all facility types',
                'recommended': False if primary_found else False  # Not recommended
            }
            
            return {
                'levels': levels_info,
                'has_levels': True,
                'total_facilities': total_facilities,
                'state_name': state  # Add state name to response
            }
            
        except Exception as e:
            logger.error(f"Error analyzing facility levels: {e}")
            return {'error': str(e), 'levels': {}}
    
    def analyze_age_groups(self, df: pd.DataFrame, state: str, facility_level: str) -> Dict[str, Any]:
        """
        Analyze age group data availability and positivity rates.
        
        Args:
            df: DataFrame with TPR data
            state: Selected state
            facility_level: Selected facility level
            
        Returns:
            Dictionary with age group statistics
        """
        try:
            # Filter data
            df = self._filter_data(df, state, facility_level)
            
            age_groups_info = {}
            
            # Log column names to help debug age group detection
            logger.info(f"Analyzing age groups for {len(df)} records with columns: {list(df.columns[:20])}")
            
            # Analyze each age group
            # EXCLUDE All Age Groups Combined per user requirement - only 3 specific groups
            # Keys MUST match what formatter expects: 'u5', 'o5', 'pw'
            age_patterns = {
                'u5': {  # Changed from 'under_5' to 'u5'
                    'name': 'Under 5 Years',
                    # Original patterns and sanitized versions
                    'test_patterns': ['<5', 'u5', 'under 5', 'under_5', '≤5',
                                    'lt_5', 'lte_5', '5yrs', '_5yr', 'lt5', 'lte5',
                                    'children', 'child'],  # More sanitized versions
                    'positive_patterns': ['positive.*<5', 'positive.*u5',
                                        'positive.*lt_5', 'positive.*lte_5',
                                        'positive.*lt5', 'positive.*children'],  # More patterns
                    'description': 'Highest risk group for severe malaria',
                    'icon': '👶'
                },
                'o5': {  # Changed from 'over_5' to 'o5'
                    'name': 'Over 5 Years',  # Simplified name
                    # Original patterns and sanitized versions
                    # Be careful not to match Under 5 patterns
                    'test_patterns': ['>5', '>=5', '≥5', 'o5', 'over 5', 'over_5',
                                    'gt_5', 'gte_5', 'gt5', 'gte5',
                                    'adult', '>5yrs', '≥5yrs', '>=5yrs', '5_years'],
                    'positive_patterns': ['positive.*>5', 'positive.*≥5', 'positive.*>=5',
                                        'positive.*o5', 'positive.*gt_5', 'positive.*gte_5',
                                        'positive.*gt5', 'positive.*gte5', 'positive.*adult'],
                    'description': 'Community transmission patterns',
                    'icon': '👥'
                },
                'pw': {  # Changed from 'pregnant' to 'pw'
                    'name': 'Pregnant Women',
                    'test_patterns': ['pregnant', 'anc', 'prenatal', 'pw', 'women',
                                    'pregnancy', 'maternal'],  # More patterns
                    'positive_patterns': ['positive.*pregnant', 'positive.*anc', 'positive.*pw',
                                        'positive.*women', 'positive.*maternal'],
                    'description': 'Special vulnerable population',
                    'icon': '🤰'
                }
            }
            
            for key, patterns in age_patterns.items():
                # Find relevant columns for this age group
                test_cols = self._find_columns_by_patterns(df, patterns['test_patterns'])
                positive_cols = self._find_columns_by_patterns(df, patterns['positive_patterns'])
                
                # Debug logging for age group detection
                if test_cols:
                    logger.debug(f"Age group '{key}': Found test columns: {test_cols[:3]}")
                else:
                    logger.debug(f"Age group '{key}': No test columns found with patterns: {patterns['test_patterns'][:5]}")
                
                # Calculate statistics WITH test type breakdown
                tests_available = 0
                positives = 0
                rdt_tests = 0
                rdt_positives = 0
                microscopy_tests = 0
                microscopy_positives = 0
                
                # Sum up test counts by type
                for col in test_cols:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        col_sum = df[col].fillna(0).sum()
                        tests_available += col_sum
                        
                        # Track by test type
                        col_lower = col.lower()
                        if 'rdt' in col_lower:
                            rdt_tests += col_sum
                        elif 'microscopy' in col_lower:
                            microscopy_tests += col_sum
                
                # Sum up positive counts by type
                for col in positive_cols:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        col_sum = df[col].fillna(0).sum()
                        positives += col_sum
                        
                        # Track by test type
                        col_lower = col.lower()
                        if 'rdt' in col_lower:
                            rdt_positives += col_sum
                        elif 'microscopy' in col_lower:
                            microscopy_positives += col_sum
                
                # Calculate positivity rates
                positivity_rate = (positives / tests_available * 100) if tests_available > 0 else 0
                rdt_tpr = (rdt_positives / rdt_tests * 100) if rdt_tests > 0 else 0
                microscopy_tpr = (microscopy_positives / microscopy_tests * 100) if microscopy_tests > 0 else 0
                
                # Count facilities with data
                facilities_with_data = 0
                for col in test_cols:
                    facilities_with_data = max(facilities_with_data, df[col].notna().sum())
                
                age_groups_info[key] = {
                    'name': patterns['name'],
                    'total_tests': int(tests_available),  # Changed from 'tests_available' to 'total_tests'
                    'total_positive': int(positives),  # Added 'total_positive' (raw count)
                    'positivity_rate': round(positivity_rate, 1),
                    'facilities_reporting': facilities_with_data,
                    'description': patterns['description'],
                    'icon': patterns['icon'],
                    'has_data': tests_available > 0,
                    # Add test type breakdown
                    'rdt_tests': int(rdt_tests),
                    'rdt_tpr': round(rdt_tpr, 1),
                    'microscopy_tests': int(microscopy_tests),
                    'microscopy_tpr': round(microscopy_tpr, 1)
                }
            
            # Remove percentage calculation - doesn't make sense when groups can overlap
            # User complained about percentages over 100%, so we'll just show absolute numbers

            # Mark Under 5 as recommended (per production standard)
            # Fixed to use correct keys: 'u5', 'o5' instead of 'under_5', 'over_5'
            if 'u5' in age_groups_info and age_groups_info['u5']['has_data']:
                age_groups_info['u5']['recommended'] = True
            elif 'o5' in age_groups_info and age_groups_info['o5']['has_data']:
                age_groups_info['o5']['recommended'] = True  # Fallback if no U5 data

            # Calculate total tests across all age groups for "all" option
            total_tests_all = sum(info.get('total_tests', 0) for info in age_groups_info.values())

            return {
                'age_groups': age_groups_info,
                'total_tests': total_tests_all,  # Added for "all age groups combined" display
                'state': state,  # Include state name for formatter
                'facility_level': facility_level  # Include facility level for formatter
            }
            
        except Exception as e:
            logger.error(f"Error analyzing age groups: {e}")
            return {'error': str(e), 'age_groups': {}}
    
    # ------------------------------------------------------------------
    # Schema inference — LLM-first, keyword fallback
    # ------------------------------------------------------------------

    def ensure_schema(self, df: pd.DataFrame) -> None:
        """Infer and cache column schema for this dataset. No-op after first call."""
        if self._schema is None:
            self._schema = self._infer_column_schema(df)

    def _infer_column_schema(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """
        Ask the LLM to map semantic field names to actual column names.

        Uses gpt-4o-mini (cheap, fast) with JSON mode. Validates every
        returned column name actually exists in df. Falls back to {} on
        any failure so _get_column() degrades to keyword detection.
        """
        try:
            api_key = self._get_openai_api_key()
            if not api_key:
                logger.warning("No OpenAI API key — schema inference skipped, using keyword fallback")
                return {}

            import openai  # local import — only needed when LLM is available

            # Build a compact sample: column names + first 3 non-empty rows
            sample = df.dropna(how='all').head(3)
            cols_preview = sample.to_string(max_cols=50, max_colwidth=60)

            prompt = (
                "You are analyzing a Nigerian malaria surveillance dataset.\n"
                "It may be a DHIS2 export, HMIS data, NMEP-processed Excel, or a custom format.\n\n"
                "Column names and sample values (first 3 rows):\n"
                f"{cols_preview}\n\n"
                "Identify the exact column name for each semantic field below.\n"
                "Return null for any field not present.\n"
                "Return ONLY the exact column name as it appears — do not rename or invent columns.\n\n"
                "Fields:\n"
                "- state: Nigerian state name\n"
                "- lga: Local Government Area\n"
                "- ward: Ward name\n"
                "- facility_name: Name of the health facility\n"
                "- facility_level: Facility tier/type (Primary, Secondary, Tertiary, PHC, etc.)\n"
                "- period: Reporting period (date, year, month, or period code)\n\n"
                'Return JSON only, no explanation:\n'
                '{"state": "...", "lga": "...", "ward": "...", '
                '"facility_name": "...", "facility_level": "...", "period": "..."}'
            )

            client = openai.OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=250,
                response_format={"type": "json_object"},
            )

            raw = response.choices[0].message.content
            parsed = json.loads(raw)

            # Validate: every value must be None or a real column in df
            validated: Dict[str, Optional[str]] = {}
            for field in _SCHEMA_FIELDS:
                val = parsed.get(field)
                if not val or val == 'null':
                    validated[field] = None
                elif val in df.columns:
                    validated[field] = val
                else:
                    logger.warning(
                        "Schema inference returned unknown column '%s' for field '%s' — ignoring",
                        val, field,
                    )
                    validated[field] = None

            logger.info("Column schema inferred: %s", validated)
            return validated

        except Exception as e:
            logger.warning("Schema inference failed (%s) — falling back to keyword detection", e)
            return {}

    def _get_openai_api_key(self) -> Optional[str]:
        """Get OpenAI API key from Flask config or environment."""
        try:
            from flask import current_app
            return current_app.config.get('OPENAI_API_KEY')
        except Exception:
            return os.environ.get('OPENAI_API_KEY')

    def _get_column(self, df: pd.DataFrame, field: str) -> Optional[str]:
        """
        Resolve a semantic field to a column name.

        Priority:
          1. LLM-inferred schema (self._schema)
          2. Keyword fallback (_keyword_fallback)
        """
        if self._schema is not None:
            col = self._schema.get(field)
            if col and col in df.columns:
                return col
        return self._keyword_fallback(df, field)

    def _keyword_fallback(self, df: pd.DataFrame, field: str) -> Optional[str]:
        """
        Original keyword-in-name detection, preserved as last-resort fallback.

        Guards added:
        - orgunitlevel* columns are never returned for facility_level
          (they are DHIS2 org hierarchy, not facility types)
        - state detection requires 1-50 unique values
        """
        keywords = _KEYWORD_MAP.get(field, [])

        for col in df.columns:
            col_lower = col.lower()

            if not any(kw in col_lower for kw in keywords):
                continue

            # DHIS2 guard: org unit hierarchy columns are never facility levels
            if field == 'facility_level' and col_lower.startswith('orgunitlevel'):
                continue

            # State column must have a plausible number of unique values
            if field == 'state':
                unique_vals = df[col].dropna().nunique()
                if not (1 <= unique_vals <= 50):
                    continue

            return col

        return None

    @staticmethod
    def _strip_dhis2_prefix(value: str) -> str:
        """Strip two-letter DHIS2 state prefixes (e.g. 'kw Kwara State' → 'Kwara')."""
        cleaned = re.sub(r'^[a-z]{2}\s+', '', str(value), flags=re.IGNORECASE)
        cleaned = re.sub(r'\s+State$', '', cleaned, flags=re.IGNORECASE)
        return cleaned.strip()

    # ------------------------------------------------------------------

    def _find_column(self, df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
        """
        Find a column matching any of the patterns.
        
        Args:
            df: DataFrame to search
            patterns: List of patterns to match
            
        Returns:
            Column name or None
        """
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None
    
    def _find_columns_by_patterns(self, df: pd.DataFrame, patterns: List[str]) -> List[str]:
        """
        Find all columns matching any of the patterns.
        Handles both exact substring matches and regex patterns.
        
        Args:
            df: DataFrame to search
            patterns: List of patterns to match
            
        Returns:
            List of column names
        """
        import re
        matching_cols = []
        
        for pattern in patterns:
            for col in df.columns:
                col_lower = col.lower()
                pattern_lower = pattern.lower()
                
                # Check if it's a regex pattern (contains .*)
                if '.*' in pattern:
                    try:
                        if re.search(pattern_lower, col_lower):
                            if col not in matching_cols:
                                matching_cols.append(col)
                    except:
                        # Fallback to substring match if regex fails
                        if pattern_lower.replace('.*', '') in col_lower and col not in matching_cols:
                            matching_cols.append(col)
                else:
                    # Simple substring match
                    if pattern_lower in col_lower and col not in matching_cols:
                        matching_cols.append(col)
        
        return matching_cols
    
    def _find_test_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Find columns that likely contain test data.
        
        Args:
            df: DataFrame to search
            
        Returns:
            List of test column names
        """
        test_patterns = ['tested', 'test', 'examined', 'screened']
        test_cols = []
        
        for col in df.columns:
            col_lower = col.lower()
            # Check if column name suggests test data
            if any(pattern in col_lower for pattern in test_patterns):
                # Verify it's numeric
                if pd.api.types.is_numeric_dtype(df[col]):
                    test_cols.append(col)
        
        return test_cols
    
    def _filter_data(self, df: pd.DataFrame, state: str, facility_level: str) -> pd.DataFrame:
        """
        Filter data by state and facility level dynamically.
        
        Args:
            df: DataFrame to filter
            state: State to filter by
            facility_level: Facility level to filter by
            
        Returns:
            Filtered DataFrame
        """
        result = df.copy()

        # Filter by state
        if state:
            state_col = self._get_column(df, 'state')
            if state_col and state in df[state_col].values:
                result = result[result[state_col] == state]

        # Filter by facility level
        if facility_level and facility_level != 'all':
            level_col = self._get_column(df, 'facility_level')
            if level_col:
                # Exact match first, then case-insensitive
                if facility_level in df[level_col].values:
                    result = result[result[level_col] == facility_level]
                elif df[level_col].astype(str).str.lower().eq(facility_level.lower()).any():
                    result = result[result[level_col].astype(str).str.lower() == facility_level.lower()]

        return result
    
    def _analyze_urban_rural(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Analyze urban/rural distribution if possible.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with urban/rural percentages
        """
        # Look for urban/rural indicator
        location_col = self._find_column(df, ['Location', 'Setting', 'Urban', 'Rural', 'Area'])
        
        if location_col:
            total = len(df)
            urban = df[location_col].str.lower().str.contains('urban', na=False).sum()
            rural = df[location_col].str.lower().str.contains('rural', na=False).sum()
            
            return {
                'urban': round((urban / total) * 100, 1) if total > 0 else 0,
                'rural': round((rural / total) * 100, 1) if total > 0 else 0
            }
        
        return {'urban': 0, 'rural': 0}
    
    def _get_facility_description(self, level: str) -> str:
        """
        Get a description for a facility level.
        
        Args:
            level: Facility level name
            
        Returns:
            Description string
        """
        level_lower = level.lower()
        
        descriptions = {
            'primary': 'Community-level care, first point of contact',
            'secondary': 'District hospitals with broader services',
            'tertiary': 'Specialized teaching hospitals',
            'phc': 'Primary Health Centers serving local communities',
            'general': 'General hospitals with comprehensive services',
            'teaching': 'Teaching hospitals with specialized care',
            'clinic': 'Small health clinics',
            'dispensary': 'Basic health dispensaries'
        }
        
        for key, desc in descriptions.items():
            if key in level_lower:
                return desc
        
        return 'Healthcare facilities'
    
    def _count_total_tests(self, df: pd.DataFrame) -> int:
        """
        Count total tests in the dataframe dynamically.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Total number of tests
        """
        total = 0
        
        # Find all columns that might contain test data
        test_patterns = ['tested', 'test', 'examined', 'screened', 'rdt', 'microscopy']
        
        for col in df.columns:
            col_lower = col.lower()
            # Check if column contains test-related keywords
            if any(pattern in col_lower for pattern in test_patterns):
                # Exclude positive/negative result columns
                if not any(exclude in col_lower for exclude in ['positive', 'negative', 'confirmed', 'result']):
                    # Check if numeric
                    if pd.api.types.is_numeric_dtype(df[col]):
                        col_sum = df[col].sum()
                        if not pd.isna(col_sum):
                            total += col_sum
        
        return int(total)
    
    def _calculate_completeness(self, df: pd.DataFrame) -> float:
        """
        Calculate data completeness percentage.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Percentage of non-null cells
        """
        if df.empty:
            return 0.0
            
        total_cells = len(df) * len(df.columns)
        if total_cells == 0:
            return 0.0
            
        non_null_cells = df.count().sum()
        return round((non_null_cells / total_cells) * 100, 1)
    
    def generate_recommendation(self, analysis: Dict[str, Any], stage: str) -> str:
        """
        Generate a recommendation based on analysis.
        
        Args:
            analysis: Analysis results
            stage: Current workflow stage
            
        Returns:
            Recommendation text
        """
        if stage == 'state':
            # Recommend state with most data
            if 'recommended' in analysis:
                return f"💡 Tip: {analysis['recommended']} has the most complete data"
        
        elif stage == 'facility':
            return "💡 Tip: 'Primary' facilities are recommended for community-level insights"
        
        elif stage == 'age':
            return "💡 Tip: 'Under 5' is the recommended age group for highest malaria risk"
        
        return ""
