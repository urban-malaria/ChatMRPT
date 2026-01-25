"""
Query Result Data Classes for ChatMRPT

Provides structured data types for query processing and formatting.
These classes enable type-safe query handling and unified formatting.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
import pandas as pd


class QueryIntent(Enum):
    """
    Classifies the user's intent from their query.

    This helps determine how to format the response:
    - COUNT: Returns a single number with context ("42 high-risk wards")
    - LIST: Returns a formatted list of items
    - EXPLAIN: Returns detailed explanation of a single item
    - COMPARE: Returns side-by-side comparison
    - AGGREGATE: Returns grouped/summarized data
    - FILTER: Returns filtered subset of data
    - UNKNOWN: Default when intent cannot be determined
    """
    COUNT = "count"           # "How many wards..."
    LIST = "list"             # "Show all wards...", "top 10..."
    EXPLAIN = "explain"       # "Why is ward X...", "details for..."
    COMPARE = "compare"       # "Compare ward X and Y..."
    AGGREGATE = "aggregate"   # "Average TPR by LGA...", "sum of..."
    FILTER = "filter"         # "Wards with score > 0.5"
    UNKNOWN = "unknown"


class ResultType(Enum):
    """
    Classifies the structure of query results.

    This determines the formatting strategy:
    - SINGLE_VALUE: One cell result (count, average, etc.)
    - RANKING: Single row with multiple columns (ward explanation)
    - LISTING: Multiple rows (top-N, filtered list)
    - AGGREGATION: Grouped results (by LGA, by state)
    - COMPARISON: Multiple specific items to compare
    - EMPTY: No results found
    """
    SINGLE_VALUE = "single_value"
    RANKING = "ranking"
    LISTING = "listing"
    AGGREGATION = "aggregation"
    COMPARISON = "comparison"
    EMPTY = "empty"


@dataclass
class QueryResult:
    """
    Structured query result for unified formatting.

    This class wraps raw query results with metadata about
    intent, type, and context to enable intelligent formatting.

    Attributes:
        data: The raw DataFrame result from the query
        sql_query: The SQL query that was executed
        original_query: The original natural language query (if available)
        intent: The detected user intent
        result_type: The detected result structure type
        context: Additional context for formatting decisions
        detected_columns: Mapping of alias types to actual column names
        visualization: Optional visualization data (for charts/maps)
    """
    data: pd.DataFrame
    sql_query: str
    original_query: str = ""
    intent: QueryIntent = QueryIntent.UNKNOWN
    result_type: ResultType = ResultType.LISTING
    context: Dict[str, Any] = field(default_factory=dict)
    detected_columns: Dict[str, str] = field(default_factory=dict)
    visualization: Optional[Dict[str, Any]] = None

    @property
    def row_count(self) -> int:
        """Number of rows in the result."""
        return len(self.data)

    @property
    def column_count(self) -> int:
        """Number of columns in the result."""
        return len(self.data.columns)

    @property
    def is_empty(self) -> bool:
        """Whether the result has no data."""
        return self.data.empty

    @property
    def is_single_value(self) -> bool:
        """Whether the result is a single value."""
        return self.row_count == 1 and self.column_count == 1

    @property
    def is_single_row(self) -> bool:
        """Whether the result is a single row (possibly multiple columns)."""
        return self.row_count == 1

    def get_single_value(self) -> Any:
        """
        Get the single value from a single-value result.

        Returns:
            The value, or None if not a single-value result
        """
        if self.is_single_value:
            return self.data.iloc[0, 0]
        return None

    def get_column(self, alias_type: str) -> Optional[str]:
        """
        Get the actual column name for an alias type.

        Args:
            alias_type: The alias type (e.g., 'ward', 'score')

        Returns:
            The actual column name, or None if not found
        """
        return self.detected_columns.get(alias_type)

    def has_column_type(self, alias_type: str) -> bool:
        """Check if a column of the given alias type exists."""
        return alias_type in self.detected_columns

    def add_context(self, key: str, value: Any) -> None:
        """Add context information for formatting."""
        self.context[key] = value

    def get_context(self, key: str, default: Any = None) -> Any:
        """Get context information."""
        return self.context.get(key, default)


@dataclass
class FormattedOutput:
    """
    The final formatted output ready for display.

    Attributes:
        text: The formatted text output (markdown)
        has_visualization: Whether a visualization was generated
        visualization_type: Type of visualization (bar, line, map, etc.)
        visualization_data: Data for rendering the visualization
        metadata: Additional metadata about the formatting
    """
    text: str
    has_visualization: bool = False
    visualization_type: Optional[str] = None
    visualization_data: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Return the text representation."""
        return self.text

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        result = {
            'text': self.text,
            'has_visualization': self.has_visualization
        }
        if self.has_visualization:
            result['visualization_type'] = self.visualization_type
            result['visualization_data'] = self.visualization_data
        if self.metadata:
            result['metadata'] = self.metadata
        return result
