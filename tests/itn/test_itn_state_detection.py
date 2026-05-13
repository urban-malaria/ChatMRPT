from types import SimpleNamespace

import pandas as pd

from app.analysis import itn_pipeline


class FakePopulationLoader:
    def get_state_code_map(self):
        return {"KW": "Kwara", "FC": "Federal Capital Territory"}

    def get_available_states(self):
        return ["Kwara", "Federal Capital Territory"]


def test_detect_state_accepts_dhis2_prefixed_state(monkeypatch):
    monkeypatch.setattr(itn_pipeline, "get_population_loader", lambda: FakePopulationLoader())
    handler = SimpleNamespace(
        shapefile_data=None,
        csv_data=pd.DataFrame({"State": ["kw Kwara State"]}),
        unified_dataset=None,
    )

    assert itn_pipeline.detect_state(handler) == "Kwara"


def test_detect_state_reads_shapefile_statename(monkeypatch):
    monkeypatch.setattr(itn_pipeline, "get_population_loader", lambda: FakePopulationLoader())
    handler = SimpleNamespace(
        shapefile_data=pd.DataFrame({"StateName": ["Kwara"]}),
        csv_data=None,
        unified_dataset=None,
    )

    assert itn_pipeline.detect_state(handler) == "Kwara"
