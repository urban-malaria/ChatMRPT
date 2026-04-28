import json
import os

import pandas as pd


def test_data_repository_basic(tmp_path):
    from app.services.data_repository import DataRepository

    repo = DataRepository(base_upload_folder=str(tmp_path))
    sid = "sess1"

    # No files yet
    assert repo.has_any_data(sid) is False
    assert repo.load_raw(sid) is None
    assert repo.load_unified(sid) is None

    # Create raw CSV
    sess_dir = tmp_path / sid
    sess_dir.mkdir(parents=True, exist_ok=True)
    (sess_dir / "raw_data.csv").write_text("Ward,TPR\nA,10\nB,20\n", encoding="utf-8")

    assert repo.has_any_data(sid) is True
    df = repo.load_raw(sid)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["Ward", "TPR"]

    # Unified dataset
    (sess_dir / "unified_dataset.csv").write_text("Ward,composite_score\nA,0.7\n", encoding="utf-8")
    udf = repo.load_unified(sid)
    assert isinstance(udf, pd.DataFrame)
    assert "composite_score" in udf.columns


def test_session_context_service_flags(tmp_path, monkeypatch):
    from app.services.data_repository import DataRepository
    from app.services.session_context import SessionContextService

    # Point uploads to tmp
    monkeypatch.setenv("UPLOAD_FOLDER", str(tmp_path))
    repo = DataRepository()
    svc = SessionContextService(repo)
    sid = "sctx1"

    # No files → not loaded
    ctx = svc.get_context(sid, {})
    assert ctx["data_loaded"] is False
    assert ctx["analysis_complete"] is False

    # Add raw CSV → data_loaded True
    sess = tmp_path / sid
    sess.mkdir(parents=True, exist_ok=True)
    (sess / "raw_data.csv").write_text("Ward,TPR\nA,10\n", encoding="utf-8")
    ctx = svc.get_context(sid, {})
    assert ctx["data_loaded"] is True
    assert ctx["csv_loaded"] is True
    assert ctx["analysis_complete"] is False

    # Add unified → analysis_complete True
    (sess / "unified_dataset.csv").write_text("Ward,composite_score\nA,0.9\n", encoding="utf-8")
    ctx = svc.get_context(sid, {})
    assert ctx["analysis_complete"] is True

    # Agent state says loaded + csv exists → flags reflect True
    (sess / ".agent_state.json").write_text(json.dumps({"data_loaded": True, "csv_loaded": True}), encoding="utf-8")
    ctx = svc.get_context(sid, {})
    assert ctx["data_loaded"] is True and ctx["csv_loaded"] is True


def test_llm_orchestrator_function_call(monkeypatch):
    from app.core.llm_orchestrator import LLMOrchestrator

    class FakeLLM:
        def generate_with_functions(self, messages, system_prompt, functions, temperature=0.7, session_id=None):
            return {"function_call": {"name": "my_tool", "arguments": json.dumps({"session_id": "abc", "x": 1})}}

    class FakeToolRunner:
        def execute(self, name, args_json):
            args = json.loads(args_json)
            return {"response": f"ok:{name}:{args.get('x')}", "status": "success", "tools_used": [name]}

    orch = LLMOrchestrator()
    res = orch.run_with_tools(FakeLLM(), "sys", "hi", [], "abc", FakeToolRunner())
    assert res["status"] == "success"
    assert res["response"].startswith("ok:my_tool:1")


def test_tool_runner_fallback(monkeypatch):
    # Stub registry to avoid heavy discovery
    import app.core.tool_runner as tr_mod

    class StubRegistry:
        def get_tool(self, name):
            return None

        def get_tool_schemas(self):
            return []

    monkeypatch.setattr(tr_mod, "get_tool_registry", lambda: StubRegistry())

    from app.core.tool_runner import ToolRunner

    def echo(session_id: str, **kw):
        return {"response": f"sid={session_id}, a={kw.get('a')}", "status": "success", "tools_used": ["echo"]}

    runner = ToolRunner(fallbacks={"echo": echo})
    out = runner.execute("echo", json.dumps({"session_id": "xyz", "a": 5}))
    assert out["status"] == "success"
    assert "sid=xyz" in out["response"]
    assert "tools_used" in out and out["tools_used"] == ["echo"]

