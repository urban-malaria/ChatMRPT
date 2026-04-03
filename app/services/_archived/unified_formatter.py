"""
Unified Query Output Formatter for ChatMRPT

Single entry point for all query result formatting.
Uses configuration-driven approach for consistent, maintainable output.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import yaml

# Support both relative and absolute imports for testability
try:
    from .query_result import QueryResult, QueryIntent, ResultType, FormattedOutput
except ImportError:
    from app.services.query_result import QueryResult, QueryIntent, ResultType, FormattedOutput

logger = logging.getLogger(__name__)


class UnifiedFormatter:
    """
    Single entry point for all query result formatting.

    This class replaces the fragmented formatting logic throughout the codebase
    with a unified, configuration-driven approach.

    Features:
    - Config-driven column aliases and friendly names
    - Intent-aware formatting (count vs list vs explain)
    - Result-type specific formatters
    - Consistent numeric precision
    - Risk factor explanations for ranking queries
    """

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the formatter with configuration.

        Args:
            config_path: Path to the YAML config file. If None, uses default.
        """
        self.config = self._load_config(config_path)
        self._build_alias_lookup()

    def _load_config(self, config_path: Optional[Path] = None) -> Dict:
        """Load configuration from YAML file."""
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config' / 'output_formatting.yaml'

        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return self._get_default_config()
        except Exception as e:
            logger.error(f"Error loading config: {e}, using defaults")
            return self._get_default_config()

    def _get_default_config(self) -> Dict:
        """Return default configuration if YAML file is unavailable."""
        return {
            'column_aliases': {
                'geographic_identifiers': {
                    'ward': ['ward', 'wardname', 'ward_name'],
                    'lga': ['lga', 'lganame', 'lga_name'],
                },
                'ranking_columns': {
                    'score': ['composite_score', 'pca_score', 'risk_score'],
                    'rank': ['composite_rank', 'pca_rank'],
                    'category': ['vulnerability_category', 'composite_category'],
                },
            },
            'friendly_names': {
                'tpr_mean': 'Test Positivity Rate',
                'composite_score': 'Risk Score',
            },
            'precision': {
                'count': 0,
                'percentage': 1,
                'score': 2,
                'rate': 2,
                'default': 2,
            },
            'thresholds': {
                'high_risk_rank': 20,
                'medium_risk_rank': 50,
                'high_percentile': 75,
                'low_percentile': 25,
            },
            'skip_patterns': ['model_', 'geometry', '_id$'],
            'risk_factors': {
                'priority_vars': ['tpr_mean', 'rainfall'],
                'inverse_risk_vars': ['distance_to_waterbodies', 'elevation_mean'],
                'max_display': 5,
            },
        }

    def _build_alias_lookup(self) -> None:
        """Build reverse lookup from column names to alias types."""
        self._alias_lookup = {}
        for category, aliases in self.config.get('column_aliases', {}).items():
            for alias_type, names in aliases.items():
                for name in names:
                    self._alias_lookup[name.lower()] = alias_type

    def format(self, result: QueryResult) -> str:
        """
        Format query result based on type and intent.

        Args:
            result: The QueryResult to format

        Returns:
            Formatted markdown string
        """
        if result.is_empty:
            return "No results found for your query."

        # Detect column types
        self._detect_columns(result)

        # Route to appropriate formatter based on result type
        formatters = {
            ResultType.SINGLE_VALUE: self._format_single_value,
            ResultType.RANKING: self._format_ranking_explanation,
            ResultType.LISTING: self._format_listing,
            ResultType.AGGREGATION: self._format_aggregation,
            ResultType.COMPARISON: self._format_comparison,
            ResultType.EMPTY: lambda r: "No results found for your query.",
        }

        formatter = formatters.get(result.result_type, self._format_general)
        try:
            return formatter(result)
        except Exception as e:
            logger.error(f"Formatting error: {e}")
            return self._format_general(result)

    def format_to_output(self, result: QueryResult) -> FormattedOutput:
        """
        Format query result and return FormattedOutput object.

        Args:
            result: The QueryResult to format

        Returns:
            FormattedOutput with text and optional visualization
        """
        text = self.format(result)
        return FormattedOutput(
            text=text,
            has_visualization=result.visualization is not None,
            visualization_type=result.visualization.get('type') if result.visualization else None,
            visualization_data=result.visualization,
            metadata={
                'intent': result.intent.value,
                'result_type': result.result_type.value,
                'row_count': result.row_count,
            }
        )

    def _detect_columns(self, result: QueryResult) -> None:
        """Detect and map column types in the result."""
        for col in result.data.columns:
            col_lower = col.lower()
            if col_lower in self._alias_lookup:
                alias_type = self._alias_lookup[col_lower]
                result.detected_columns[alias_type] = col

    def _format_single_value(self, result: QueryResult) -> str:
        """Format single value results with context."""
        value = result.get_single_value()
        col_name = result.data.columns[0]

        # Get friendly name
        friendly = self._get_friendly_name(col_name)

        # Format value with appropriate precision
        formatted_value = self._format_value(value, col_name)

        # Add context based on intent
        if result.intent == QueryIntent.COUNT:
            # Determine unit from context
            unit = result.get_context('unit', 'items')
            return f"**{friendly}:** {formatted_value} {unit}"
        else:
            return f"**{friendly}:** {formatted_value}"

    def _format_listing(self, result: QueryResult) -> str:
        """Format multi-row listing results."""
        lines = []
        df = result.data

        # Get key columns
        ward_col = result.get_column('ward')
        score_col = result.get_column('score')
        category_col = result.get_column('category')
        lga_col = result.get_column('lga')
        rank_col = result.get_column('rank')

        # Header
        lines.append(f"**Results ({len(df)} items):**\n")

        # Format each row
        for idx, row in df.iterrows():
            # Determine rank number
            if rank_col and pd.notna(row.get(rank_col)):
                rank_num = int(row[rank_col])
            elif isinstance(idx, int):
                rank_num = idx + 1
            else:
                rank_num = df.index.get_loc(idx) + 1

            # Primary identifier
            if ward_col and pd.notna(row.get(ward_col)):
                name = row[ward_col]
            else:
                # Use first string column
                name = self._get_primary_identifier(row, df.columns)

            # Build info parts
            parts = []

            if score_col and pd.notna(row.get(score_col)):
                score = row[score_col]
                formatted_score = self._format_value(score, score_col)
                parts.append(f"Score: {formatted_score}")

            if category_col and pd.notna(row.get(category_col)):
                parts.append(f"({row[category_col]})")

            if lga_col and pd.notna(row.get(lga_col)):
                parts.append(f"in {row[lga_col]}")

            # Format line
            if parts:
                info = " - ".join(parts)
                lines.append(f"{rank_num}. **{name}** {info}")
            else:
                lines.append(f"{rank_num}. **{name}**")

        return "\n".join(lines)

    def _format_ranking_explanation(self, result: QueryResult, full_df: pd.DataFrame = None) -> str:
        """
        Format single-row ranking explanation with risk factors.

        This provides detailed explanation of why a ward has its ranking.
        """
        row = result.data.iloc[0]

        # Get ward name
        ward_col = result.get_column('ward')
        ward_name = row[ward_col] if ward_col else "Selected ward"

        # Get ranking info
        rank_col = result.get_column('rank')
        category_col = result.get_column('category')

        rank_value = int(row[rank_col]) if rank_col and pd.notna(row.get(rank_col)) else None
        category_value = row[category_col] if category_col and pd.notna(row.get(category_col)) else "Unknown"

        # Build output
        lines = []

        # Header
        if rank_value:
            lines.append(f"{ward_name} ward is ranked #{rank_value} ({category_value})")
        else:
            lines.append(f"{ward_name} ward - {category_value}")

        # Risk factors (if full dataset is available for comparison)
        if full_df is not None:
            risk_factors = self._extract_risk_factors(row, full_df, result.data.columns)
            if risk_factors:
                lines.append("")
                lines.append("Top Risk Factors:")
                for factor in risk_factors:
                    lines.append(f"• {factor['name']}: {factor['formatted_value']} ({factor['level']})")

        # Recommendation
        lines.append("")
        recommendation = self._get_recommendation(rank_value)
        lines.append(recommendation)

        return "\n".join(lines)

    def _format_aggregation(self, result: QueryResult) -> str:
        """Format aggregation results (GROUP BY, etc.)."""
        lines = []
        df = result.data

        lines.append(f"**Aggregation Results ({len(df)} groups):**\n")

        for idx, row in df.iterrows():
            parts = []
            for col in df.columns:
                value = row[col]
                friendly = self._get_friendly_name(col)
                formatted = self._format_value(value, col)
                parts.append(f"{friendly}: {formatted}")
            lines.append(f"• {' | '.join(parts)}")

        return "\n".join(lines)

    def _format_comparison(self, result: QueryResult) -> str:
        """Format comparison between multiple items."""
        lines = []
        df = result.data

        ward_col = result.get_column('ward')

        lines.append(f"**Comparison ({len(df)} items):**\n")

        for idx, row in df.iterrows():
            name = row[ward_col] if ward_col else f"Item {idx + 1}"
            lines.append(f"### {name}")

            for col in df.columns:
                if col == ward_col:
                    continue
                if self._should_skip_column(col):
                    continue

                value = row[col]
                friendly = self._get_friendly_name(col)
                formatted = self._format_value(value, col)
                lines.append(f"• {friendly}: {formatted}")

            lines.append("")

        return "\n".join(lines)

    def _format_general(self, result: QueryResult) -> str:
        """Fallback general formatting for any result."""
        lines = []
        df = result.data

        if len(df) <= 10:
            for idx, row in df.iterrows():
                parts = []
                for col in df.columns:
                    if self._should_skip_column(col):
                        continue
                    value = row[col]
                    formatted = self._format_value(value, col)
                    parts.append(f"{col}: {formatted}")
                lines.append(f"• {' - '.join(parts)}")
        else:
            lines.append(f"Found {len(df)} results. Showing first 5:\n")
            for idx, row in df.head(5).iterrows():
                parts = []
                for col in df.columns:
                    if self._should_skip_column(col):
                        continue
                    value = row[col]
                    formatted = self._format_value(value, col)
                    parts.append(f"{formatted}")
                lines.append(f"• {', '.join(parts)}")
            lines.append("\n*Use a more specific query to narrow down results.*")

        return "\n".join(lines)

    def _get_friendly_name(self, col_name: str) -> str:
        """Get friendly display name for a column."""
        friendly_names = self.config.get('friendly_names', {})

        # Check exact match
        if col_name in friendly_names:
            return friendly_names[col_name]

        # Check case-insensitive
        for key, value in friendly_names.items():
            if key.lower() == col_name.lower():
                return value

        # Default: convert snake_case to Title Case
        return col_name.replace('_', ' ').title()

    def _format_value(self, value: Any, col_name: str) -> str:
        """Format a value with appropriate precision."""
        if pd.isna(value):
            return "N/A"

        if isinstance(value, float):
            precision = self._get_precision(col_name)
            if value >= 1000:
                return f"{value:,.{precision}f}"
            elif value < 1:
                return f"{value:.{precision}f}"
            else:
                return f"{value:.{precision}f}"
        elif isinstance(value, int):
            if value >= 1000:
                return f"{value:,}"
            return str(value)
        else:
            return str(value)

    def _get_precision(self, col_name: str) -> int:
        """Get numeric precision for a column based on patterns."""
        col_lower = col_name.lower()
        precision_config = self.config.get('precision', {})
        precision_patterns = self.config.get('precision_patterns', {})

        for precision_type, patterns in precision_patterns.items():
            for pattern in patterns:
                if pattern in col_lower or re.search(pattern, col_lower):
                    return precision_config.get(precision_type, 2)

        return precision_config.get('default', 2)

    def _should_skip_column(self, col_name: str) -> bool:
        """Check if a column should be skipped in output."""
        skip_patterns = self.config.get('skip_patterns', [])
        col_lower = col_name.lower()

        for pattern in skip_patterns:
            if re.search(pattern, col_lower):
                return True
        return False

    def _get_primary_identifier(self, row: pd.Series, columns: List[str]) -> str:
        """Get the primary identifier from a row."""
        # Try common identifier columns
        for col in columns:
            if any(term in col.lower() for term in ['name', 'ward', 'lga', 'state']):
                value = row.get(col)
                if pd.notna(value):
                    return str(value)
        # Fall back to first non-numeric column
        for col in columns:
            value = row.get(col)
            if pd.notna(value) and isinstance(value, str):
                return value
        # Last resort
        return str(row.iloc[0])

    def _extract_risk_factors(
        self,
        row: pd.Series,
        full_df: pd.DataFrame,
        columns: List[str]
    ) -> List[Dict[str, Any]]:
        """Extract top risk factors for a ward."""
        risk_config = self.config.get('risk_factors', {})
        priority_vars = risk_config.get('priority_vars', [])
        inverse_vars = risk_config.get('inverse_risk_vars', [])
        max_display = risk_config.get('max_display', 5)

        skip_cols = set()
        # Build skip set from column aliases
        for category, aliases in self.config.get('column_aliases', {}).items():
            for alias_type, names in aliases.items():
                if alias_type in ['ward', 'lga', 'state', 'score', 'rank', 'category']:
                    skip_cols.update(name.lower() for name in names)

        risk_factors = []

        for col in columns:
            col_lower = col.lower()

            # Skip administrative columns
            if col_lower in skip_cols:
                continue

            # Skip patterns
            if self._should_skip_column(col):
                continue

            value = row.get(col)
            if pd.isna(value) or col not in full_df.columns:
                continue

            # Only process numeric columns
            if not pd.api.types.is_numeric_dtype(full_df[col]):
                continue

            try:
                col_values = full_df[col].dropna()
                n_values = len(col_values)
                if n_values == 0:
                    continue

                # Calculate percentile
                percentile = (col_values <= value).sum() / n_values * 100

                # Only include if extreme (>75th or <25th percentile)
                thresholds = self.config.get('thresholds', {})
                high_pct = thresholds.get('high_percentile', 75)
                low_pct = thresholds.get('low_percentile', 25)

                if percentile > high_pct or percentile < low_pct:
                    friendly_name = self._get_friendly_name(col)

                    # Determine if it's a risk factor
                    if col in inverse_vars or col_lower in [v.lower() for v in inverse_vars]:
                        is_risk = percentile < low_pct
                    else:
                        is_risk = percentile > high_pct

                    # Determine level
                    very_high_pct = thresholds.get('very_high_percentile', 90)
                    very_low_pct = thresholds.get('very_low_percentile', 10)

                    if percentile > very_high_pct:
                        level = "Very high"
                    elif percentile > high_pct:
                        level = "High"
                    elif percentile < very_low_pct:
                        level = "Very low"
                    else:
                        level = "Low"

                    risk_factors.append({
                        'name': friendly_name,
                        'value': value,
                        'formatted_value': self._format_value(value, col),
                        'percentile': percentile,
                        'is_risk': is_risk,
                        'level': level,
                        'priority': col in priority_vars or col_lower in [v.lower() for v in priority_vars],
                    })

            except Exception as e:
                logger.debug(f"Error processing column {col}: {e}")
                continue

        # Sort by priority, then by percentile extremity
        risk_factors.sort(
            key=lambda x: (not x['priority'], -abs(x['percentile'] - 50))
        )

        # Filter to only risk factors and limit
        risk_factors = [f for f in risk_factors if f['is_risk']][:max_display]

        return risk_factors

    def _get_recommendation(self, rank: Optional[int]) -> str:
        """Get recommendation based on rank."""
        thresholds = self.config.get('thresholds', {})
        templates = self.config.get('templates', {}).get('recommendation', {})

        high_risk_rank = thresholds.get('high_risk_rank', 20)
        medium_risk_rank = thresholds.get('medium_risk_rank', 50)

        if rank and rank <= high_risk_rank:
            return templates.get(
                'high_risk',
                "Recommendation: Priority area for immediate intervention - ITN distribution and vector control needed."
            )
        elif rank and rank <= medium_risk_rank:
            return templates.get(
                'medium_risk',
                "Recommendation: High-priority area for malaria interventions."
            )
        else:
            return templates.get(
                'low_risk',
                "Recommendation: Monitor situation and maintain preventive measures."
            )


# Module-level singleton for convenience
_formatter_instance: Optional[UnifiedFormatter] = None


def get_formatter() -> UnifiedFormatter:
    """Get or create the global formatter instance."""
    global _formatter_instance
    if _formatter_instance is None:
        _formatter_instance = UnifiedFormatter()
    return _formatter_instance


def format_query_result(result: QueryResult) -> str:
    """Convenience function to format a query result."""
    return get_formatter().format(result)
