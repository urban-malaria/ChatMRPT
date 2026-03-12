"""
Shared pytest fixtures for TPR tests.

Patches _infer_column_schema to return {} so tests never make real OpenAI calls.
Schema-specific tests inject schemas directly on the analyzer instance instead.
"""
import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def no_llm_schema_inference():
    """Prevent live OpenAI calls during tests. Forces keyword fallback."""
    with patch(
        "app.data_analysis_v3.tpr.data_analyzer.TPRDataAnalyzer._infer_column_schema",
        return_value={},
    ):
        yield
