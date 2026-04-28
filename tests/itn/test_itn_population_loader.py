"""Tests for the unified ITN population loader."""

import types
import sys

import pytest

# Provide lightweight stubs for optional Flask extensions used in app/__init__.py
flask_session_stub = types.ModuleType("flask_session")


class _SessionStub:  # pragma: no cover - simple placeholder
    def __init__(self, *args, **kwargs):
        pass


flask_session_stub.Session = _SessionStub
sys.modules.setdefault("flask_session", flask_session_stub)

from app.planning.population_loader import get_population_loader


def test_population_loader_lists_all_states():
    loader = get_population_loader()
    states = loader.get_available_states()
    # The national dataset should contain all 36 states plus FCT
    assert 'Kano' in states
    assert 'Lagos' in states
    assert 'Federal Capital Territory' in states
    assert len(states) >= 36


def test_population_loader_resolves_state_by_code_and_name():
    loader = get_population_loader()

    kano_by_code = loader.load_state_population('KN')
    assert kano_by_code is not None and not kano_by_code.empty
    assert set(kano_by_code['StateCode'].unique()) == {'KN'}
    assert 'WardCode' in kano_by_code.columns

    kano_by_name = loader.load_state_population('Kano')
    assert kano_by_name is not None and not kano_by_name.empty
    assert set(kano_by_name['StateCode'].unique()) == {'KN'}
    assert kano_by_code['Population'].sum() == kano_by_name['Population'].sum()


@pytest.mark.parametrize('identifier', ['Federal Capital Territory', 'FCT', 'Abuja', 'Abuja State'])
def test_population_loader_handles_fct_aliases(identifier):
    loader = get_population_loader()
    fct_df = loader.load_state_population(identifier)
    assert fct_df is not None and not fct_df.empty
    assert set(fct_df['StateCode'].unique()) == {'FC'}
