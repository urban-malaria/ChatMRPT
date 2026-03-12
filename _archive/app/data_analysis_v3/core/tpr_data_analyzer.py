"""
TPR Data Analyzer

Analyzes TPR data to provide rich contextual information at each decision point.
Generates statistics for states, facilities, and age groups to help users make informed choices.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


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
            
            # Find state column dynamically - check all columns
            state_col = None
            for col in df.columns:
                col_lower = col.lower()
                # Check for state-related keywords
                if any(keyword in col_lower for keyword in ['state', 'region', 'province', 'area']):
                    # Verify it has reasonable number of unique values for states
                    unique_vals = df[col].dropna().nunique()
                    if 1 <= unique_vals <= 50:  # Reasonable range for states
                        state_col = col
                        logger.info(f"Found state column: {col}")
                        break
            
            if not state_col:
                # If no state column, analyze as single entity
                logger.info("No state column found, analyzing as single dataset")
                return {
                    'states': {'All Data': {
                        'name': 'All Data',
                        'total_records': len(df),
                        'facilities': len(df),
                        'total_tests': self._count_total_tests(df),
                        'data_completeness': self._calculate_completeness(df)
                    }},
                    'total_states': 1,
                    'recommended': 'All Data'
                }
            
            # Find facility column dynamically
            facility_col = None
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['facility', 'clinic', 'hospital', 'health', 'center']):
                    facility_col = col
                    logger.info(f"Found facility column: {col}")
                    break
            
            # Find test columns dynamically
            test_cols = self._find_test_columns(df)
            
            # Analyze each state
            for state in df[state_col].dropna().unique():
                state_data = df[df[state_col] == state]
                
                state_info = {
                    'name': str(state),
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
                'recommended': sorted_states[0][0] if sorted_states else None
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
            # Filter to selected state dynamically
            state_col = None
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['state', 'region', 'province']):
                    # Verify this column has the state we're looking for
                    if state in df[col].values:
                        state_col = col
                        break
            
            if state_col:
                df = df[df[state_col] == state]
            
            # Find facility level column dynamically
            level_col = None
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['facility', 'level', 'type', 'tier', 'category']):
                    # Check if it has reasonable categorical values
                    unique_vals = df[col].dropna().nunique()
                    if 2 <= unique_vals <= 20:  # Reasonable range for facility types
                        level_col = col
                        break
            
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

            # Find facility identifier column (e.g., HealthFacility, FacilityName)
            facility_id_col = None
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['healthfacility', 'facility_name', 'facilityname']):
                    facility_id_col = col
                    break

            # Count unique facilities (not rows)
            if facility_id_col:
                total_facilities = df[facility_id_col].nunique()
                logger.info(f"Found {total_facilities} unique facilities (from {len(df)} records)")
            else:
                # Fallback: count rows if no facility ID column
                total_facilities = len(df)
                logger.warning(f"No facility ID column found, counting rows instead: {total_facilities}")

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

                # Count unique facilities for this level
                if facility_id_col:
                    facility_count = level_data[facility_id_col].nunique()
                else:
                    facility_count = len(level_data)

                levels_info[level_key] = {
                    'name': level,
                    'count': facility_count,
                    'percentage': round((facility_count / total_facilities) * 100, 1),
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
            age_patterns = {
                'under_5': {
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
                'over_5': {
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
                'pregnant': {
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
                
                # Count unique facilities with data for this age group
                # Find facility identifier column
                facility_id_col = None
                for col in df.columns:
                    col_lower = col.lower()
                    if any(keyword in col_lower for keyword in ['healthfacility', 'facility_name', 'facilityname']):
                        facility_id_col = col
                        break

                facilities_with_data = 0
                if facility_id_col and test_cols:
                    # Count unique facilities that have at least one non-null test value
                    mask = False
                    for col in test_cols:
                        if col in df.columns:
                            mask = mask | df[col].notna()
                    if isinstance(mask, pd.Series):
                        facilities_with_data = df[mask][facility_id_col].nunique()
                    else:
                        # Fallback if mask is still False
                        facilities_with_data = 0
                elif test_cols:
                    # Fallback: count rows with data
                    for col in test_cols:
                        facilities_with_data = max(facilities_with_data, df[col].notna().sum())

                age_groups_info[key] = {
                    'name': patterns['name'],
                    'tests_available': int(tests_available),
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
            if 'under_5' in age_groups_info and age_groups_info['under_5']['has_data']:
                age_groups_info['under_5']['recommended'] = True
            elif 'over_5' in age_groups_info and age_groups_info['over_5']['has_data']:
                age_groups_info['over_5']['recommended'] = True  # Fallback if no U5 data
            
            return {
                'age_groups': age_groups_info,
                'state': state,  # Include state name for formatter
                'facility_level': facility_level  # Include facility level for formatter
            }
            
        except Exception as e:
            logger.error(f"Error analyzing age groups: {e}")
            return {'error': str(e), 'age_groups': {}}
    
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
        
        # Filter by state dynamically
        if state:
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['state', 'region', 'province']):
                    if state in df[col].values:
                        result = result[result[col] == state]
                        break
        
        # Filter by facility level dynamically
        if facility_level and facility_level != 'all':
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['facility', 'level', 'type', 'tier']):
                    # Try exact match first
                    if facility_level in df[col].values:
                        result = result[result[col] == facility_level]
                        break
                    # Try case-insensitive match
                    elif df[col].astype(str).str.lower().eq(facility_level.lower()).any():
                        result = result[result[col].astype(str).str.lower() == facility_level.lower()]
                        break
        
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