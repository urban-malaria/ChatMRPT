"""
Query Intent Analyzer for ChatMRPT

Analyzes SQL queries and natural language to determine user intent.
This enables intelligent formatting of query results.
"""

import re
import logging
from typing import Optional, Tuple
import pandas as pd

# Support both relative and absolute imports for testability
try:
    from .query_result import QueryIntent, ResultType
except ImportError:
    from app.services.query_result import QueryIntent, ResultType

logger = logging.getLogger(__name__)


class QueryIntentAnalyzer:
    """
    Analyzes SQL queries to determine user intent and result type.

    This class examines both the SQL structure and the original natural
    language query (if available) to classify:
    1. What the user wants to know (intent)
    2. What kind of result they'll get (result type)
    """

    def __init__(self):
        # Natural language patterns for intent detection
        self.nl_patterns = {
            QueryIntent.COUNT: [
                r'\bhow many\b',
                r'\bcount\b',
                r'\bnumber of\b',
                r'\btotal\s+(?:number|count)\b',
            ],
            QueryIntent.LIST: [
                r'\btop\s+\d+\b',
                r'\bshow\s+(?:me\s+)?(?:all|the)\b',
                r'\blist\b',
                r'\bwhich\s+wards\b',
                r'\bwhat\s+are\s+the\b',
                r'\bhighest\b',
                r'\blowest\b',
            ],
            QueryIntent.EXPLAIN: [
                r'\bwhy\s+is\b',
                r'\bexplain\b',
                r'\bdetails?\s+(?:for|about|on)\b',
                r'\btell\s+me\s+about\b',
                r'\bwhat\s+makes\b',
            ],
            QueryIntent.COMPARE: [
                r'\bcompare\b',
                r'\bdifference\s+between\b',
                r'\bvs\.?\b',
                r'\bversus\b',
                r'\band\b.*\band\b',  # "X and Y and Z"
            ],
            QueryIntent.AGGREGATE: [
                r'\baverage\b',
                r'\bmean\b',
                r'\bsum\b',
                r'\btotal\b',
                r'\bby\s+(?:lga|state|region)\b',
                r'\bper\s+(?:lga|state|region)\b',
                r'\bgrouped?\b',
            ],
            QueryIntent.FILTER: [
                r'\bwith\b.*\b(?:greater|less|more|above|below|over|under)\b',
                r'\bwhere\b',
                r'\b(?:greater|less)\s+than\b',
                r'\bhigh\s+risk\b',
                r'\blow\s+risk\b',
            ],
        }

    def analyze(self, sql_query: str, original_query: str = "") -> QueryIntent:
        """
        Determine intent from SQL structure and original query.

        Args:
            sql_query: The generated SQL query
            original_query: The original natural language query

        Returns:
            The detected QueryIntent
        """
        # First try natural language patterns if available
        if original_query:
            nl_intent = self._analyze_natural_language(original_query)
            if nl_intent != QueryIntent.UNKNOWN:
                logger.debug(f"Intent from NL: {nl_intent.value}")
                return nl_intent

        # Fall back to SQL analysis
        sql_intent = self._analyze_sql(sql_query)
        logger.debug(f"Intent from SQL: {sql_intent.value}")
        return sql_intent

    def _analyze_natural_language(self, query: str) -> QueryIntent:
        """Analyze natural language query for intent."""
        query_lower = query.lower()

        # Check each intent pattern
        for intent, patterns in self.nl_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    return intent

        return QueryIntent.UNKNOWN

    def _analyze_sql(self, sql_query: str) -> QueryIntent:
        """Analyze SQL structure for intent."""
        sql_upper = sql_query.upper()

        # COUNT queries
        if re.search(r'SELECT\s+COUNT\s*\(', sql_upper):
            return QueryIntent.COUNT

        # Aggregate queries (AVG, SUM, etc.)
        if re.search(r'SELECT\s+(?:AVG|SUM|MIN|MAX)\s*\(', sql_upper):
            return QueryIntent.AGGREGATE

        # GROUP BY indicates aggregation
        if 'GROUP BY' in sql_upper:
            return QueryIntent.AGGREGATE

        # Single row with WHERE on identifier (explanation query)
        if re.search(r"WHERE\s+\w*(?:ward|name)\w*\s*=", sql_upper, re.IGNORECASE):
            # Check if it's asking about a specific ward
            if 'LIMIT 1' in sql_upper or not re.search(r'LIMIT\s+\d+', sql_upper):
                return QueryIntent.EXPLAIN

        # Comparison (multiple specific items via IN)
        if re.search(r"WHERE.*IN\s*\(", sql_upper):
            return QueryIntent.COMPARE

        # Ranking/Top-N queries
        if re.search(r'ORDER BY.*(?:DESC|ASC).*LIMIT', sql_upper):
            return QueryIntent.LIST

        # Filter queries (WHERE without specific identifier)
        if 'WHERE' in sql_upper:
            return QueryIntent.FILTER

        # Default to list
        return QueryIntent.LIST

    def classify_result_type(
        self,
        df: pd.DataFrame,
        intent: QueryIntent,
        sql_query: str = ""
    ) -> ResultType:
        """
        Classify the result type based on data shape and intent.

        Args:
            df: The result DataFrame
            intent: The detected query intent
            sql_query: The SQL query (for additional context)

        Returns:
            The detected ResultType
        """
        if df.empty:
            return ResultType.EMPTY

        row_count = len(df)
        col_count = len(df.columns)

        # Single value (COUNT, AVG, etc.)
        if row_count == 1 and col_count == 1:
            return ResultType.SINGLE_VALUE

        # Single row with multiple columns (ward explanation)
        if row_count == 1:
            if intent == QueryIntent.EXPLAIN:
                return ResultType.RANKING
            # Could also be a single aggregation result
            if intent == QueryIntent.AGGREGATE:
                return ResultType.AGGREGATION
            return ResultType.RANKING

        # Aggregation results
        if intent == QueryIntent.AGGREGATE:
            return ResultType.AGGREGATION

        # Comparison results
        if intent == QueryIntent.COMPARE:
            return ResultType.COMPARISON

        # Default to listing for multiple rows
        return ResultType.LISTING

    def analyze_full(
        self,
        df: pd.DataFrame,
        sql_query: str,
        original_query: str = ""
    ) -> Tuple[QueryIntent, ResultType]:
        """
        Perform full analysis returning both intent and result type.

        Args:
            df: The result DataFrame
            sql_query: The SQL query
            original_query: The original natural language query

        Returns:
            Tuple of (QueryIntent, ResultType)
        """
        intent = self.analyze(sql_query, original_query)
        result_type = self.classify_result_type(df, intent, sql_query)
        return intent, result_type


# Module-level convenience function
def analyze_query_intent(
    sql_query: str,
    original_query: str = ""
) -> QueryIntent:
    """
    Convenience function to analyze query intent.

    Args:
        sql_query: The SQL query
        original_query: The original natural language query

    Returns:
        The detected QueryIntent
    """
    analyzer = QueryIntentAnalyzer()
    return analyzer.analyze(sql_query, original_query)
