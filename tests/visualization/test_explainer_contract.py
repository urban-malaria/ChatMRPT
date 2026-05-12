import json

import pandas as pd
from flask import Flask

from app.visualization.explainer import UniversalVisualizationExplainer


def test_vulnerability_prompt_contains_grounding_guardrails(tmp_path):
    app = Flask(__name__)
    upload_root = tmp_path / "uploads"
    session_dir = upload_root / "session-1"
    session_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "WardName": ["Afon", "Ibagun", "Share"],
            "LGA": ["Asa", "Ilorin East", "Ifelodun"],
            "rainfall": [10.0, 20.0, 15.0],
            "ndvi": [0.2, 0.5, 0.3],
            "composite_score": [0.7, 0.9, 0.4],
            "composite_rank": [2, 1, 3],
            "composite_category": ["High", "High", "Medium"],
        }
    ).to_csv(session_dir / "unified_dataset.csv", index=False)

    app.config["UPLOAD_FOLDER"] = str(upload_root)
    with app.app_context():
        explainer = UniversalVisualizationExplainer()
        viz_type = explainer._normalize_viz_type("vulnerability_map", "vulnerability_map.html")
        ctx = explainer._build_data_context("session-1", viz_type, "vulnerability_map.html")
        prompt = explainer._build_prompt(viz_type, ctx)

    assert viz_type == "vulnerability_map_composite"
    assert "Use only the evidence listed below" in prompt
    assert "do not mention sanitation" in prompt.lower()
    assert "rainfall" in prompt
    assert "Ibagun" in prompt


def test_itn_context_includes_saved_allocation_results(tmp_path):
    app = Flask(__name__)
    upload_root = tmp_path / "uploads"
    session_dir = upload_root / "session-1"
    session_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "WardName": ["Ibagun", "Essa B"],
            "LGA": ["Ilorin East", "Ilorin West"],
            "composite_score": [0.9, 0.8],
            "composite_rank": [1, 2],
            "rainfall": [20.0, 18.0],
        }
    ).to_csv(session_dir / "unified_dataset.csv", index=False)
    (session_dir / "itn_distribution_results.json").write_text(
        json.dumps(
            {
                "stats": {
                    "total_nets": 300000,
                    "allocated_nets": 300000,
                    "coverage_percent": 14.0,
                    "prioritized_wards": 33,
                },
                "prioritized": [
                    {
                        "WardName": "Ibagun",
                        "overall_rank": 1,
                        "nets_allocated": 42402,
                        "Population": 76324,
                        "coverage_percent": 100.0,
                    }
                ],
            }
        )
    )

    app.config["UPLOAD_FOLDER"] = str(upload_root)
    with app.app_context():
        explainer = UniversalVisualizationExplainer()
        ctx = explainer._build_data_context("session-1", "itn_map", "itn_map.html")
        prompt = explainer._build_prompt("itn_map", ctx)

    assert ctx["itn_stats"]["total_nets"] == 300000
    assert ctx["top_itn_allocations"][0]["ward"] == "Ibagun"
    assert "42,402 nets" in prompt


def test_unsupported_causal_claims_are_removed():
    explainer = UniversalVisualizationExplainer()
    ctx = {
        "viz_type": "vulnerability_map_composite",
        "variables_used": ["rainfall", "ndvi"],
        "allowed_evidence": ["WardName", "LGA", "composite_rank"],
    }

    cleaned = explainer._remove_unsupported_claims(
        "## Interpretation\nHigh-risk wards may reflect poor sanitation and limited healthcare access. "
        "Rainfall is available in the model.",
        ctx,
    )

    assert "poor sanitation" not in cleaned
    assert "limited healthcare access" not in cleaned
    assert "Rainfall is available" in cleaned
