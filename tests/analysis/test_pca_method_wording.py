from pathlib import Path


def test_pca_skipped_wording_requires_both_tests_to_pass():
    source = Path("app/analysis/complete_tools.py").read_text()

    assert "PCA is used only when **both** tests pass" in source
    assert "PCA was not used because the suitability checks did not both pass" in source
    assert "minimum threshold: 0.5" in source
    assert "borderline for PCA by KMO alone" in source
