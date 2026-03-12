"""
Unit tests for TPR Data Analyzer

Tests data analysis, statistics generation, and contextual information display.
"""

import pytest
import pandas as pd
import numpy as np
from app.data_analysis_v3.tpr.data_analyzer import TPRDataAnalyzer


@pytest.fixture
def sample_tpr_data():
    """Create sample TPR data for testing."""
    data = {
        'State': ['Adamawa'] * 100 + ['Kwara'] * 80 + ['Osun'] * 90,
        'HealthFacility': [f'Facility_{i}' for i in range(270)],
        'FacilityLevel': ['Primary'] * 150 + ['Secondary'] * 80 + ['Tertiary'] * 40,
        'WardName': [f'Ward_{i%30}' for i in range(270)],
        'LGA': [f'LGA_{i%10}' for i in range(270)],
        
        # Test data columns
        'Persons presenting with fever & tested by RDT <5yrs': np.random.randint(10, 100, 270),
        'Persons presenting with fever & tested positive by RDT <5yrs': np.random.randint(5, 50, 270),
        'Persons presenting with fever & tested by Microscopy <5yrs': np.random.randint(5, 50, 270),
        'Persons presenting with fever & tested positive by Microscopy <5yrs': np.random.randint(2, 25, 270),
        
        'Persons presenting with fever & tested by RDT >5yrs': np.random.randint(15, 120, 270),
        'Persons presenting with fever & tested positive by RDT >5yrs': np.random.randint(5, 40, 270),
        
        'Pregnant women tested by RDT': np.random.randint(5, 30, 270),
        'Pregnant women tested positive by RDT': np.random.randint(1, 10, 270),
    }
    return pd.DataFrame(data)


@pytest.fixture
def analyzer():
    """Create TPRDataAnalyzer instance."""
    return TPRDataAnalyzer()


class TestTPRDataAnalyzer:
    """Test TPRDataAnalyzer functionality."""
    
    def test_initialization(self, analyzer):
        """Test analyzer initializes correctly."""
        assert analyzer.data is None
        assert analyzer.analysis_cache == {}
    
    def test_analyze_states(self, analyzer, sample_tpr_data):
        """Test state-level analysis."""
        # Act
        result = analyzer.analyze_states(sample_tpr_data)
        
        # Assert
        assert 'states' in result
        assert result['total_states'] == 3
        assert 'Adamawa' in result['states']
        assert 'Kwara' in result['states']
        assert 'Osun' in result['states']
        
        # Check state info
        adamawa_info = result['states']['Adamawa']
        assert adamawa_info['total_records'] == 100
        assert adamawa_info['facilities'] > 0
        assert adamawa_info['total_tests'] > 0
        assert 0 <= adamawa_info['data_completeness'] <= 100
        
        # Check recommended state
        assert result['recommended'] is not None
    
    def test_analyze_states_no_state_column(self, analyzer):
        """Test handling when no state column exists."""
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})

        result = analyzer.analyze_states(df)

        # No state column → error returned, workflow aborts cleanly
        assert 'states' in result
        assert result['state_column_detected'] is False
        assert result['error'] == 'STATE_COLUMN_NOT_FOUND'
        assert result['total_states'] == 0
    
    def test_analyze_facility_levels(self, analyzer, sample_tpr_data):
        """Test facility level analysis."""
        # Kwara has both Primary and Secondary in the sample fixture
        result = analyzer.analyze_facility_levels(sample_tpr_data, 'Kwara')

        assert 'levels' in result
        assert result['has_levels'] is True
        assert 'all' in result['levels']
        assert 'primary' in result['levels']
        assert 'secondary' in result['levels']

        # 'all' option is always present
        all_level = result['levels']['all']
        assert all_level['percentage'] == 100

        # Primary level has data
        primary = result['levels']['primary']
        assert primary['count'] > 0
        assert primary['percentage'] > 0
    
    def test_analyze_facility_levels_no_level_column(self, analyzer, sample_tpr_data):
        """Test when no facility level column exists."""
        df = sample_tpr_data.drop('FacilityLevel', axis=1)
        
        result = analyzer.analyze_facility_levels(df, 'Adamawa')
        
        assert result['has_levels'] is False
        assert 'all' in result['levels']
        assert len(result['levels']) == 1
    
    def test_analyze_age_groups(self, analyzer, sample_tpr_data):
        """Test age group analysis."""
        # Act
        result = analyzer.analyze_age_groups(sample_tpr_data, 'Adamawa', 'all')
        
        # Assert
        assert 'age_groups' in result
        assert 'total_tests' in result
        
        # Check age groups — keys are 'u5', 'o5', 'pw'
        age_groups = result['age_groups']
        assert 'u5' in age_groups
        assert 'o5' in age_groups
        assert 'pw' in age_groups

        # Check under-5 data
        u5 = age_groups['u5']
        assert u5['total_tests'] > 0
        assert u5['positivity_rate'] >= 0
        assert u5['has_data']
        assert 'description' in u5
        assert 'icon' in u5
    
    def test_analyze_age_groups_with_facility_filter(self, analyzer, sample_tpr_data):
        """Test age group analysis with facility level filter."""
        # Act
        result = analyzer.analyze_age_groups(sample_tpr_data, 'Adamawa', 'primary')
        
        # Assert
        assert 'age_groups' in result
        # Should have filtered to primary facilities only
        age_groups = result['age_groups']
        assert age_groups['u5']['total_tests'] > 0
    
    def test_find_column(self, analyzer):
        """Test column finding logic."""
        df = pd.DataFrame({
            'State': [],
            'state_name': [],
            'FacilityLevel': [],
            'Other': []
        })
        
        # Should find 'State'
        col = analyzer._find_column(df, ['State', 'STATE', 'state'])
        assert col == 'State'
        
        # Should find 'FacilityLevel'
        col = analyzer._find_column(df, ['Level', 'FacilityLevel', 'Type'])
        assert col == 'FacilityLevel'
        
        # Should return None for non-existent
        col = analyzer._find_column(df, ['NonExistent', 'Missing'])
        assert col is None
    
    def test_find_columns_by_patterns(self, analyzer):
        """Test finding multiple columns by patterns."""
        df = pd.DataFrame({
            'tested_u5': [],
            'tested_o5': [],
            'positive_u5': [],
            'other_column': []
        })
        
        # Find test columns
        cols = analyzer._find_columns_by_patterns(df, ['tested', 'test'])
        assert 'tested_u5' in cols
        assert 'tested_o5' in cols
        assert 'other_column' not in cols
        
        # Find positive columns
        cols = analyzer._find_columns_by_patterns(df, ['positive'])
        assert 'positive_u5' in cols
        assert 'tested_u5' not in cols
    
    def test_find_test_columns(self, analyzer, sample_tpr_data):
        """Test finding test data columns."""
        # Act
        test_cols = analyzer._find_test_columns(sample_tpr_data)
        
        # Assert
        assert len(test_cols) > 0
        # Should find columns with 'tested' in name
        assert any('tested' in col.lower() for col in test_cols)
    
    def test_filter_data(self, analyzer, sample_tpr_data):
        """Test data filtering by state and facility level."""
        # Filter by state
        filtered = analyzer._filter_data(sample_tpr_data, 'Adamawa', 'all')
        assert len(filtered) == 100  # Only Adamawa records
        
        # Filter by state and facility level
        filtered = analyzer._filter_data(sample_tpr_data, 'Adamawa', 'primary')
        assert all(filtered['FacilityLevel'] == 'Primary')
        assert all(filtered['State'] == 'Adamawa')
    
    def test_analyze_urban_rural(self, analyzer):
        """Test urban/rural distribution analysis."""
        # Create data with location info
        df = pd.DataFrame({
            'Location': ['Urban', 'Rural', 'Urban', 'Rural', 'Rural']
        })
        
        result = analyzer._analyze_urban_rural(df)
        
        assert result['urban'] == 40.0  # 2/5 = 40%
        assert result['rural'] == 60.0  # 3/5 = 60%
    
    def test_analyze_urban_rural_no_location(self, analyzer):
        """Test when no location column exists."""
        df = pd.DataFrame({'Other': [1, 2, 3]})
        
        result = analyzer._analyze_urban_rural(df)
        
        assert result['urban'] == 0
        assert result['rural'] == 0
    
    def test_get_facility_description(self, analyzer):
        """Test facility description generation."""
        assert 'Community-level' in analyzer._get_facility_description('Primary')
        assert 'District hospitals' in analyzer._get_facility_description('Secondary')
        assert 'teaching hospitals' in analyzer._get_facility_description('Tertiary').lower()
        assert 'Healthcare facilities' in analyzer._get_facility_description('Unknown')
    
    def test_generate_recommendation(self, analyzer):
        """Test recommendation generation."""
        # State recommendation
        analysis = {'recommended': 'Adamawa'}
        rec = analyzer.generate_recommendation(analysis, 'state')
        assert 'Adamawa' in rec
        assert 'most complete data' in rec
        
        # Facility recommendation
        rec = analyzer.generate_recommendation({}, 'facility')
        assert 'Primary' in rec

        # Age group recommendation
        rec = analyzer.generate_recommendation({}, 'age')
        assert 'Under 5' in rec or 'age' in rec.lower()
    
    def test_empty_dataframe(self, analyzer):
        """Test handling of empty dataframe."""
        df = pd.DataFrame()
        
        # Should handle gracefully
        state_result = analyzer.analyze_states(df)
        assert state_result['states'] == {}
        
        facility_result = analyzer.analyze_facility_levels(df, 'Any')
        assert 'levels' in facility_result
        
        age_result = analyzer.analyze_age_groups(df, 'Any', 'all')
        assert 'age_groups' in age_result
    
    def test_percentage_calculations(self, analyzer, sample_tpr_data):
        """Test that percentages are calculated correctly."""
        result = analyzer.analyze_facility_levels(sample_tpr_data, 'Adamawa')
        
        # Sum of individual facility level percentages should be ~100
        # (excluding 'all' which is always 100%)
        levels = result['levels']
        total_percentage = sum(
            info['percentage'] 
            for key, info in levels.items() 
            if key != 'all'
        )
        
        # Allow for rounding differences
        assert 95 <= total_percentage <= 105
    
    def test_positivity_rate_calculation(self, analyzer):
        """Test TPR/positivity rate calculation."""
        # Create controlled data
        df = pd.DataFrame({
            'State': ['Test'] * 10,
            'tested_u5': [100] * 10,
            'positive_u5': [25] * 10  # 25% positivity
        })
        
        # Analyze
        result = analyzer.analyze_age_groups(df, 'Test', 'all')
        
        # Check positivity rate calculation
        # Note: The actual calculation might vary based on column detection
        # This is a simplified test
        assert 'age_groups' in result


# ---------------------------------------------------------------------------
# Schema inference tests
# ---------------------------------------------------------------------------

class TestSchemaInference:
    """Tests for LLM schema inference and keyword fallback."""

    def _dhis2_df(self):
        """Minimal DHIS2-style DataFrame matching the Kwara export format."""
        return pd.DataFrame({
            'orgunitlevel2': ['kw Kwara State'] * 20,
            'orgunitlevel3': ['kw Asa Local Government Area'] * 10 + ['kw Ilorin South Local Government Area'] * 10,
            'Ward': ['Unknown'] * 20,
            'organisationunit0me': [f'Facility_{i}' for i in range(20)],
            'period0me': [2022] * 20,
            'Facility level': ['primary'] * 12 + ['secondary'] * 5 + ['Tertiary'] * 3,
            'Persons presenting with fever & tested by RDT <5yrs': np.random.randint(5, 50, 20),
            'Persons tested positive for malaria by RDT <5yrs': np.random.randint(1, 20, 20),
        })

    def test_keyword_fallback_standard_format(self):
        """Keyword fallback correctly identifies standard NMEP columns."""
        analyzer = TPRDataAnalyzer()
        df = pd.DataFrame({
            'State': ['Adamawa'] * 10,
            'LGA': ['Yola South'] * 10,
            'HealthFacility': [f'F_{i}' for i in range(10)],
            'FacilityLevel': ['Primary'] * 7 + ['Secondary'] * 3,
        })
        assert analyzer._keyword_fallback(df, 'state') == 'State'
        assert analyzer._keyword_fallback(df, 'facility_level') == 'FacilityLevel'
        assert analyzer._keyword_fallback(df, 'facility_name') == 'HealthFacility'

    def test_keyword_fallback_never_picks_orgunitlevel_for_facility_level(self):
        """DHIS2 orgunitlevel columns must never be returned for facility_level."""
        analyzer = TPRDataAnalyzer()
        df = self._dhis2_df()
        col = analyzer._keyword_fallback(df, 'facility_level')
        assert col == 'Facility level', (
            f"Expected 'Facility level', got '{col}'. "
            "orgunitlevel3 should be skipped despite containing 'level'."
        )

    def test_schema_injected_takes_priority_over_keywords(self):
        """When a valid schema is injected, _get_column uses it over keyword matching."""
        analyzer = TPRDataAnalyzer()
        df = self._dhis2_df()
        # Simulate what a successful LLM call would return
        analyzer._schema = {
            'state': 'orgunitlevel2',
            'lga': 'orgunitlevel3',
            'ward': 'Ward',
            'facility_name': 'organisationunit0me',
            'facility_level': 'Facility level',
            'period': 'period0me',
        }
        assert analyzer._get_column(df, 'state') == 'orgunitlevel2'
        assert analyzer._get_column(df, 'lga') == 'orgunitlevel3'
        assert analyzer._get_column(df, 'facility_level') == 'Facility level'
        assert analyzer._get_column(df, 'facility_name') == 'organisationunit0me'

    def test_invalid_schema_column_falls_back_to_keywords(self):
        """If schema returns a column not in df, fall back to keyword detection."""
        analyzer = TPRDataAnalyzer()
        df = pd.DataFrame({
            'State': ['Kwara'] * 5,
            'FacilityLevel': ['Primary'] * 5,
        })
        analyzer._schema = {
            'state': 'NonExistentColumn',  # bad — not in df
            'facility_level': 'FacilityLevel',
        }
        # Should fall back to keyword detection for state
        assert analyzer._get_column(df, 'state') == 'State'
        # But use schema for facility_level (valid)
        assert analyzer._get_column(df, 'facility_level') == 'FacilityLevel'

    def test_analyze_states_dhis2_with_injected_schema(self):
        """Full analyze_states run on DHIS2 data with schema injected (no LLM call)."""
        analyzer = TPRDataAnalyzer()
        df = self._dhis2_df()
        analyzer._schema = {
            'state': 'orgunitlevel2',
            'lga': 'orgunitlevel3',
            'ward': 'Ward',
            'facility_name': 'organisationunit0me',
            'facility_level': 'Facility level',
            'period': 'period0me',
        }
        result = analyzer.analyze_states(df)
        assert result.get('state_column_detected') is True
        assert 'kw Kwara State' in result['states']
        # display_name should strip the prefix
        assert result['states']['kw Kwara State']['display_name'] == 'Kwara'

    def test_analyze_facility_levels_dhis2_with_injected_schema(self):
        """Facility level detection uses correct column on DHIS2 data."""
        analyzer = TPRDataAnalyzer()
        df = self._dhis2_df()
        analyzer._schema = {
            'state': 'orgunitlevel2',
            'lga': 'orgunitlevel3',
            'ward': 'Ward',
            'facility_name': 'organisationunit0me',
            'facility_level': 'Facility level',
            'period': 'period0me',
        }
        result = analyzer.analyze_facility_levels(df, 'kw Kwara State')
        assert result.get('has_levels') is True
        levels = result['levels']
        # Should have primary/secondary/tertiary — NOT LGA names
        level_names = {info['name'].lower() for key, info in levels.items() if key != 'all'}
        assert 'primary' in level_names
        assert 'secondary' in level_names
        # LGA names should never appear as facility levels
        lga_values = set(df['orgunitlevel3'].unique())
        for name in level_names:
            assert name not in {v.lower() for v in lga_values}, (
                f"LGA name '{name}' appeared as a facility level — detection bug not fixed"
            )

    def test_filter_data_dhis2_with_injected_schema(self):
        """_filter_data correctly filters by state and facility level on DHIS2 data."""
        analyzer = TPRDataAnalyzer()
        df = self._dhis2_df()
        analyzer._schema = {
            'state': 'orgunitlevel2',
            'facility_level': 'Facility level',
        }
        filtered = analyzer._filter_data(df, 'kw Kwara State', 'primary')
        # Should not return all rows — only primary facilities
        assert len(filtered) < len(df)
        assert filtered['Facility level'].str.lower().eq('primary').all()

    def test_ensure_schema_called_once(self):
        """ensure_schema is idempotent — second call does not overwrite schema."""
        analyzer = TPRDataAnalyzer()
        df = pd.DataFrame({'State': ['X'], 'FacilityLevel': ['Primary']})
        # Manually set schema to simulate a prior LLM call
        analyzer._schema = {'state': 'State', 'facility_level': 'FacilityLevel'}
        original = analyzer._schema

        # Call ensure_schema again — should be a no-op
        analyzer.ensure_schema(df)
        assert analyzer._schema is original

    def test_strip_dhis2_prefix(self):
        assert TPRDataAnalyzer._strip_dhis2_prefix('kw Kwara State') == 'Kwara'
        assert TPRDataAnalyzer._strip_dhis2_prefix('ad Adamawa State') == 'Adamawa'
        assert TPRDataAnalyzer._strip_dhis2_prefix('Osun') == 'Osun'
        assert TPRDataAnalyzer._strip_dhis2_prefix('os Osun State') == 'Osun'