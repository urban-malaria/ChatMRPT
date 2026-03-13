"""
Shared pytest fixtures for TPR tests.

Patches _call_llm_schema to return a minimal schema so tests never make real
OpenAI calls.  Schema-specific tests inject schemas directly on the analyzer
instance via tpr_analyzer._schema = {...} instead.
"""
import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def no_llm_schema_inference():
    """Prevent live OpenAI calls during tests."""
    minimal_schema = {
        "header_row": 0,
        "state": None,
        "lga": None,
        "ward": None,
        "facility_name": None,
        "facility_level": None,
        "period": None,
        "u5_rdt_tested": None,
        "u5_rdt_positive": None,
        "o5_rdt_tested": None,
        "o5_rdt_positive": None,
        "pw_rdt_tested": None,
        "pw_rdt_positive": None,
        "u5_microscopy_tested": None,
        "u5_microscopy_positive": None,
        "o5_microscopy_tested": None,
        "o5_microscopy_positive": None,
        "pw_microscopy_tested": None,
        "pw_microscopy_positive": None,
    }
    with patch(
        "app.data_analysis_v3.tpr.data_analyzer.TPRDataAnalyzer._call_llm_schema",
        return_value=minimal_schema,
    ):
        yield
