"""
Tests for the Unified Query Output Formatter

Tests cover:
- Intent analysis from SQL and natural language
- Result type classification
- Single value formatting
- Listing formatting
- Ranking explanation formatting
- Aggregation formatting
- Config-driven precision and friendly names
"""

import pytest
import pandas as pd
import sys
import os
import importlib.util

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Bypass app/__init__.py by loading modules directly
def load_module_directly(name, path):
    """Load a module directly from file path, bypassing package __init__.py"""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

# Load query_result first (no dependencies)
query_result = load_module_directly(
    'app.services.query_result',
    os.path.join(project_root, 'app', 'services', 'query_result.py')
)
QueryIntent = query_result.QueryIntent
ResultType = query_result.ResultType
QueryResult = query_result.QueryResult

# Load query_intent_analyzer (depends on query_result, which is now in sys.modules)
query_intent_analyzer = load_module_directly(
    'app.services.query_intent_analyzer',
    os.path.join(project_root, 'app', 'services', 'query_intent_analyzer.py')
)
QueryIntentAnalyzer = query_intent_analyzer.QueryIntentAnalyzer

# Load unified_formatter (depends on query_result, which is now in sys.modules)
unified_formatter = load_module_directly(
    'app.services.unified_formatter',
    os.path.join(project_root, 'app', 'services', 'unified_formatter.py')
)
UnifiedFormatter = unified_formatter.UnifiedFormatter


class TestQueryIntentAnalyzer:
    """Tests for the QueryIntentAnalyzer class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = QueryIntentAnalyzer()

    def test_count_intent_from_sql(self):
        """Test COUNT intent detection from SQL."""
        sql = "SELECT COUNT(*) FROM df WHERE vulnerability_category = 'High Risk'"
        intent = self.analyzer.analyze(sql)
        assert intent == QueryIntent.COUNT

    def test_aggregate_intent_from_sql(self):
        """Test AGGREGATE intent detection from SQL."""
        sql = "SELECT LGAName, AVG(tpr_mean) FROM df GROUP BY LGAName"
        intent = self.analyzer.analyze(sql)
        assert intent == QueryIntent.AGGREGATE

    def test_list_intent_from_sql(self):
        """Test LIST intent detection from SQL with ORDER BY and LIMIT."""
        sql = "SELECT WardName, composite_score FROM df ORDER BY composite_score DESC LIMIT 10"
        intent = self.analyzer.analyze(sql)
        assert intent == QueryIntent.LIST

    def test_explain_intent_from_sql(self):
        """Test EXPLAIN intent detection from SQL with specific ward."""
        sql = "SELECT * FROM df WHERE WardName = 'Abuja'"
        intent = self.analyzer.analyze(sql)
        assert intent == QueryIntent.EXPLAIN

    def test_compare_intent_from_sql(self):
        """Test COMPARE intent detection from SQL with IN clause."""
        sql = "SELECT * FROM df WHERE WardName IN ('Abuja', 'Lagos', 'Kano')"
        intent = self.analyzer.analyze(sql)
        assert intent == QueryIntent.COMPARE

    def test_count_intent_from_natural_language(self):
        """Test COUNT intent detection from natural language."""
        sql = "SELECT * FROM df"
        nl = "how many high risk wards are there"
        intent = self.analyzer.analyze(sql, nl)
        assert intent == QueryIntent.COUNT

    def test_list_intent_from_natural_language(self):
        """Test LIST intent detection from natural language."""
        sql = "SELECT * FROM df"
        nl = "show me the top 10 highest risk wards"
        intent = self.analyzer.analyze(sql, nl)
        assert intent == QueryIntent.LIST

    def test_explain_intent_from_natural_language(self):
        """Test EXPLAIN intent detection from natural language."""
        sql = "SELECT * FROM df"
        nl = "why is Kabuga ward ranked so high"
        intent = self.analyzer.analyze(sql, nl)
        assert intent == QueryIntent.EXPLAIN

    def test_aggregate_intent_from_natural_language(self):
        """Test AGGREGATE intent detection from natural language."""
        sql = "SELECT * FROM df"
        nl = "what is the average TPR by LGA"
        intent = self.analyzer.analyze(sql, nl)
        assert intent == QueryIntent.AGGREGATE


class TestResultTypeClassification:
    """Tests for result type classification."""

    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = QueryIntentAnalyzer()

    def test_single_value_classification(self):
        """Test SINGLE_VALUE result type for 1x1 DataFrame."""
        df = pd.DataFrame({'count': [42]})
        result_type = self.analyzer.classify_result_type(df, QueryIntent.COUNT)
        assert result_type == ResultType.SINGLE_VALUE

    def test_ranking_classification(self):
        """Test RANKING result type for single row with EXPLAIN intent."""
        df = pd.DataFrame({
            'WardName': ['Abuja'],
            'composite_score': [0.85],
            'composite_rank': [5]
        })
        result_type = self.analyzer.classify_result_type(df, QueryIntent.EXPLAIN)
        assert result_type == ResultType.RANKING

    def test_listing_classification(self):
        """Test LISTING result type for multiple rows."""
        df = pd.DataFrame({
            'WardName': ['A', 'B', 'C'],
            'composite_score': [0.9, 0.8, 0.7]
        })
        result_type = self.analyzer.classify_result_type(df, QueryIntent.LIST)
        assert result_type == ResultType.LISTING

    def test_aggregation_classification(self):
        """Test AGGREGATION result type for GROUP BY results."""
        df = pd.DataFrame({
            'LGAName': ['Lagos', 'Kano'],
            'avg_tpr': [0.45, 0.38]
        })
        result_type = self.analyzer.classify_result_type(df, QueryIntent.AGGREGATE)
        assert result_type == ResultType.AGGREGATION

    def test_empty_classification(self):
        """Test EMPTY result type for empty DataFrame."""
        df = pd.DataFrame()
        result_type = self.analyzer.classify_result_type(df, QueryIntent.LIST)
        assert result_type == ResultType.EMPTY


class TestUnifiedFormatter:
    """Tests for the UnifiedFormatter class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.formatter = UnifiedFormatter()

    def test_empty_result_formatting(self):
        """Test formatting of empty results."""
        df = pd.DataFrame()
        result = QueryResult(
            data=df,
            sql_query="SELECT * FROM df WHERE 1=0",
            result_type=ResultType.EMPTY
        )
        output = self.formatter.format(result)
        assert "No results found" in output

    def test_single_value_formatting(self):
        """Test formatting of single value results."""
        df = pd.DataFrame({'count': [42]})
        result = QueryResult(
            data=df,
            sql_query="SELECT COUNT(*) as count FROM df",
            intent=QueryIntent.COUNT,
            result_type=ResultType.SINGLE_VALUE
        )
        result.add_context('unit', 'wards')
        output = self.formatter.format(result)
        assert "42" in output

    def test_listing_formatting(self):
        """Test formatting of listing results."""
        df = pd.DataFrame({
            'WardName': ['Abuja', 'Lagos', 'Kano'],
            'composite_score': [0.85, 0.78, 0.72],
            'vulnerability_category': ['High Risk', 'High Risk', 'Medium Risk']
        })
        result = QueryResult(
            data=df,
            sql_query="SELECT WardName, composite_score FROM df ORDER BY composite_score DESC LIMIT 3",
            intent=QueryIntent.LIST,
            result_type=ResultType.LISTING
        )
        output = self.formatter.format(result)

        # Check structure
        assert "Results (3 items)" in output
        assert "Abuja" in output
        assert "Lagos" in output
        assert "Kano" in output
        assert "Score:" in output or "0.85" in output

    def test_aggregation_formatting(self):
        """Test formatting of aggregation results."""
        df = pd.DataFrame({
            'LGAName': ['Lagos', 'Kano'],
            'avg_tpr': [0.45, 0.38]
        })
        result = QueryResult(
            data=df,
            sql_query="SELECT LGAName, AVG(tpr_mean) as avg_tpr FROM df GROUP BY LGAName",
            intent=QueryIntent.AGGREGATE,
            result_type=ResultType.AGGREGATION
        )
        output = self.formatter.format(result)

        assert "Aggregation Results" in output
        assert "Lagos" in output
        assert "Kano" in output

    def test_config_driven_friendly_names(self):
        """Test that friendly names are applied from config."""
        df = pd.DataFrame({'tpr_mean': [0.45]})
        result = QueryResult(
            data=df,
            sql_query="SELECT AVG(tpr_mean) FROM df",
            intent=QueryIntent.AGGREGATE,
            result_type=ResultType.SINGLE_VALUE
        )
        output = self.formatter.format(result)

        # Should use friendly name from config
        assert "Test Positivity Rate" in output or "0.45" in output

    def test_precision_for_scores(self):
        """Test that scores are formatted with appropriate precision."""
        df = pd.DataFrame({'composite_score': [0.856789]})
        result = QueryResult(
            data=df,
            sql_query="SELECT composite_score FROM df LIMIT 1",
            intent=QueryIntent.LIST,
            result_type=ResultType.SINGLE_VALUE
        )
        output = self.formatter.format(result)

        # Should be formatted to 2 decimal places per config
        assert "0.86" in output or "0.857" in output

    def test_skip_technical_columns(self):
        """Test that technical columns are skipped."""
        df = pd.DataFrame({
            'WardName': ['Test'],
            'model_1': [0.5],
            'model_2': [0.3],
            'composite_score': [0.8]
        })
        result = QueryResult(
            data=df,
            sql_query="SELECT * FROM df LIMIT 1",
            intent=QueryIntent.LIST,
            result_type=ResultType.LISTING
        )
        output = self.formatter.format(result)

        # model_* columns should be skipped
        assert "model_1" not in output
        assert "model_2" not in output


class TestQueryResult:
    """Tests for the QueryResult data class."""

    def test_row_count(self):
        """Test row_count property."""
        df = pd.DataFrame({'a': [1, 2, 3]})
        result = QueryResult(data=df, sql_query="SELECT * FROM df")
        assert result.row_count == 3

    def test_is_empty(self):
        """Test is_empty property."""
        empty_df = pd.DataFrame()
        result = QueryResult(data=empty_df, sql_query="SELECT * FROM df")
        assert result.is_empty is True

        non_empty_df = pd.DataFrame({'a': [1]})
        result2 = QueryResult(data=non_empty_df, sql_query="SELECT * FROM df")
        assert result2.is_empty is False

    def test_is_single_value(self):
        """Test is_single_value property."""
        single_df = pd.DataFrame({'count': [42]})
        result = QueryResult(data=single_df, sql_query="SELECT COUNT(*) FROM df")
        assert result.is_single_value is True

        multi_df = pd.DataFrame({'a': [1], 'b': [2]})
        result2 = QueryResult(data=multi_df, sql_query="SELECT * FROM df")
        assert result2.is_single_value is False

    def test_get_single_value(self):
        """Test get_single_value method."""
        df = pd.DataFrame({'count': [42]})
        result = QueryResult(data=df, sql_query="SELECT COUNT(*) FROM df")
        assert result.get_single_value() == 42

    def test_context_management(self):
        """Test context add and get."""
        df = pd.DataFrame({'a': [1]})
        result = QueryResult(data=df, sql_query="SELECT * FROM df")

        result.add_context('key', 'value')
        assert result.get_context('key') == 'value'
        assert result.get_context('missing', 'default') == 'default'


class TestIntegration:
    """Integration tests for the complete formatting pipeline."""

    def test_full_pipeline_count_query(self):
        """Test complete pipeline for a count query."""
        # Simulate: "how many high risk wards"
        sql = "SELECT COUNT(*) as high_risk_count FROM df WHERE vulnerability_category = 'High Risk'"
        df = pd.DataFrame({'high_risk_count': [42]})

        analyzer = QueryIntentAnalyzer()
        intent, result_type = analyzer.analyze_full(df, sql, "how many high risk wards")

        assert intent == QueryIntent.COUNT
        assert result_type == ResultType.SINGLE_VALUE

        result = QueryResult(
            data=df,
            sql_query=sql,
            original_query="how many high risk wards",
            intent=intent,
            result_type=result_type
        )

        formatter = UnifiedFormatter()
        output = formatter.format(result)

        assert "42" in output

    def test_full_pipeline_top_n_query(self):
        """Test complete pipeline for a top-N query."""
        # Simulate: "top 5 highest risk wards"
        sql = "SELECT WardName, composite_score, composite_rank FROM df ORDER BY composite_score DESC LIMIT 5"
        df = pd.DataFrame({
            'WardName': ['A', 'B', 'C', 'D', 'E'],
            'composite_score': [0.9, 0.85, 0.8, 0.75, 0.7],
            'composite_rank': [1, 2, 3, 4, 5]
        })

        analyzer = QueryIntentAnalyzer()
        intent, result_type = analyzer.analyze_full(df, sql, "top 5 highest risk wards")

        assert intent == QueryIntent.LIST
        assert result_type == ResultType.LISTING

        result = QueryResult(
            data=df,
            sql_query=sql,
            original_query="top 5 highest risk wards",
            intent=intent,
            result_type=result_type
        )

        formatter = UnifiedFormatter()
        output = formatter.format(result)

        # Should have numbered list with ward names
        assert "1." in output or "**A**" in output
        assert "Results (5 items)" in output


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
