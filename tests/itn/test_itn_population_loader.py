"""Tests for the unified ITN population loader."""

from pathlib import Path
import types
import sys

import pytest

# Provide lightweight stubs for optional Flask extensions used in app/__init__.py
flask_compress_stub = types.ModuleType("flask_compress")
flask_login_stub = types.ModuleType("flask_login")
flask_session_stub = types.ModuleType("flask_session")


class _CompressStub:  # pragma: no cover - simple placeholder
    def __init__(self, *args, **kwargs):
        pass


class _LoginManagerStub:  # pragma: no cover - simple placeholder
    def __init__(self, *args, **kwargs):
        pass


class _SessionStub:  # pragma: no cover - simple placeholder
    def __init__(self, *args, **kwargs):
        pass


flask_compress_stub.Compress = _CompressStub
flask_login_stub.LoginManager = _LoginManagerStub
flask_session_stub.Session = _SessionStub
sys.modules.setdefault("flask_compress", flask_compress_stub)
sys.modules.setdefault("flask_login", flask_login_stub)
sys.modules.setdefault("flask_session", flask_session_stub)

from app.planning.population_loader import ITNPopulationLoader, get_population_loader


REPO_ROOT = Path(__file__).resolve().parents[2]
POPULATION_CSV = REPO_ROOT / "www" / "wards_with_pop.csv"
requires_population_csv = pytest.mark.skipif(
    not POPULATION_CSV.exists(),
    reason="www/wards_with_pop.csv is not present in this checkout",
)


def test_population_loader_resolves_paths_from_repo_root():
    loader = ITNPopulationLoader()
    assert loader.base_path == REPO_ROOT
    assert loader.population_csv_path == REPO_ROOT / "www" / "wards_with_pop.csv"
    assert loader.shapefile_path == REPO_ROOT / "www" / "complete_names_wards" / "wards.shp"


@requires_population_csv
def test_population_loader_lists_all_states():
    loader = get_population_loader()
    states = loader.get_available_states()
    # The national dataset should contain all 36 states plus FCT
    assert 'Kano' in states
    assert 'Lagos' in states
    assert 'Federal Capital Territory' in states
    assert len(states) >= 36


@requires_population_csv
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


@requires_population_csv
@pytest.mark.parametrize('identifier', ['Federal Capital Territory', 'FCT', 'Abuja', 'Abuja State'])
def test_population_loader_handles_fct_aliases(identifier):
    loader = get_population_loader()
    fct_df = loader.load_state_population(identifier)
    assert fct_df is not None and not fct_df.empty
    assert set(fct_df['StateCode'].unique()) == {'FC'}
