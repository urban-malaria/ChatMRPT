"""Tests for burden analysis bundle ZIP creation."""
import zipfile
from pathlib import Path

import pytest

from app.tpr.workflow_manager import _create_burden_package
from app.tpr.export_helpers import write_burden_readme


# ---------------------------------------------------------------------------
# write_burden_readme
# ---------------------------------------------------------------------------

def test_readme_contains_state_and_date():
    content = write_burden_readme('Kwara', '2026-05-18', [
        'ward_malaria_burden_environmental_variables.csv',
        'data_dictionary.csv',
    ])
    assert 'Kwara' in content
    assert '2026-05-18' in content


def test_readme_only_lists_included_files():
    content = write_burden_readme('Lagos', '2026-05-18', [
        'ward_malaria_burden_environmental_variables.csv',
    ])
    assert 'ward_malaria_burden_environmental_variables.csv' in content
    assert 'ward_population_denominators.csv' not in content


# ---------------------------------------------------------------------------
# _create_burden_package
# ---------------------------------------------------------------------------

def _write_session_files(session_folder: Path, files: list):
    session_folder.mkdir(parents=True)
    for name in files:
        (session_folder / name).write_text(f"data for {name}", encoding='utf-8')


def test_bundle_created_with_all_four_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_folder = tmp_path / 'instance' / 'uploads' / 'sess1'
    _write_session_files(session_folder, [
        'raw_data.csv', 'raw_shapefile.zip', 'population_data.csv', 'data_dictionary.csv'
    ])

    result = _create_burden_package(session_folder, 'sess1', 'Kwara')

    assert result is not None
    assert result['type'] == 'zip'
    assert result['filename'].startswith('malaria_burden_analysis_Kwara_')
    assert result['filename'].endswith('.zip')
    assert 'sess1' in result['url']

    bundle_path = session_folder / result['filename']
    assert bundle_path.exists()

    with zipfile.ZipFile(bundle_path) as zf:
        names = zf.namelist()

    assert 'ward_malaria_burden_environmental_variables.csv' in names
    assert 'ward_boundaries_burden_environmental_variables.zip' in names
    assert 'ward_population_denominators.csv' in names
    assert 'data_dictionary.csv' in names
    assert 'README.txt' in names


def test_bundle_partial_when_files_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_folder = tmp_path / 'instance' / 'uploads' / 'sess2'
    _write_session_files(session_folder, ['raw_data.csv', 'data_dictionary.csv'])

    result = _create_burden_package(session_folder, 'sess2', 'Kano')

    assert result is not None
    bundle_path = session_folder / result['filename']
    with zipfile.ZipFile(bundle_path) as zf:
        names = zf.namelist()

    assert 'ward_malaria_burden_environmental_variables.csv' in names
    assert 'data_dictionary.csv' in names
    assert 'ward_boundaries_burden_environmental_variables.zip' not in names
    assert 'README.txt' in names
    assert '2 files' in result['description']


def test_bundle_returns_none_when_no_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_folder = tmp_path / 'instance' / 'uploads' / 'sess3'
    session_folder.mkdir(parents=True)

    result = _create_burden_package(session_folder, 'sess3', 'Lagos')
    assert result is None


def test_bundle_returns_none_on_write_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_folder = tmp_path / 'instance' / 'uploads' / 'sess4'
    _write_session_files(session_folder, ['raw_data.csv'])

    # Make the session folder read-only so ZipFile.write fails
    session_folder.chmod(0o555)
    try:
        result = _create_burden_package(session_folder, 'sess4', 'Oyo')
        assert result is None
    finally:
        session_folder.chmod(0o755)


def test_readme_inside_bundle_mentions_state(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_folder = tmp_path / 'instance' / 'uploads' / 'sess5'
    _write_session_files(session_folder, ['raw_data.csv'])

    result = _create_burden_package(session_folder, 'sess5', 'Borno')

    bundle_path = session_folder / result['filename']
    with zipfile.ZipFile(bundle_path) as zf:
        readme = zf.read('README.txt').decode('utf-8')

    assert 'Borno' in readme
    assert 'ChatMRPT' in readme
