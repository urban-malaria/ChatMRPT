#!/usr/bin/env python3
"""
ChatMRPT Codebase Restructuring Script

Moves 131 files, updates all imports, creates __init__.py files.
Run from project root. Creates one atomic git commit.

Usage:
    python migrate_structure.py --dry-run   # Preview changes
    python migrate_structure.py --execute   # Do it
"""

import os
import re
import sys
import shutil
import subprocess
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).parent
APP_DIR = PROJECT_ROOT / "app"

# ═══════════════════════════════════════════════════════════════
# FILE MAPPING: old_path → new_path (relative to app/)
# ═══════════════════════════════════════════════════════════════

FILE_MOVES = {
    # ── core/ → distributed ──────────────────────────────────
    "core/llm_manager.py": "services/llm_manager.py",
    "core/llm_adapter.py": "services/llm_adapter.py",
    "core/request_interpreter.py": "agent/interpreter.py",
    "core/prompt_builder.py": "agent/prompt_builder.py",
    "core/session_context_service.py": "services/session_context.py",
    "core/data_repository.py": "services/data_repository.py",
    "core/unified_data_state.py": "services/data_state.py",
    "core/session_state.py": "conversation/session_state.py",
    "core/workflow_state_manager.py": "conversation/workflow_state.py",
    "core/analysis_state_handler.py": "conversation/analysis_state.py",
    "core/session_helper.py": "conversation/session_helper.py",
    "core/arena_manager.py": "arena/manager.py",
    "core/arena_system_prompt.py": "arena/prompts.py",
    "core/tpr_precompute.py": "tpr/precompute.py",
    "core/tpr_precompute_service.py": "tpr/precompute_service.py",
    "core/tpr_utils.py": "tpr/utils.py",
    "core/tpr_ward_cache.py": "tpr/cache.py",
    "core/decorators.py": "utils/decorators.py",
    "core/exceptions.py": "utils/exceptions.py",
    "core/responses.py": "utils/responses.py",
    "core/utils.py": "utils/core_utils.py",
    "core/variable_matcher.py": "utils/variable_matcher.py",
    "core/tool_validator.py": "utils/tool_validator.py",
    "core/dependency_validator.py": "utils/dependency_validator.py",
    "core/tool_schema_registry.py": "utils/tool_schema_registry.py",
    "core/redis_state_manager.py": "services/redis_state.py",
    "core/instance_sync.py": "services/instance_sync.py",

    # ── data_analysis_v3/ → agent/ + tpr/ ────────────────────
    "data_analysis_v3/core/agent.py": "agent/agent.py",
    "data_analysis_v3/core/data_exploration_agent.py": "agent/exploration_agent.py",
    "data_analysis_v3/core/executor.py": "agent/executor.py",
    "data_analysis_v3/core/executor_simple.py": "agent/executor_simple.py",
    "data_analysis_v3/core/formatters.py": "agent/formatters.py",
    "data_analysis_v3/core/data_profiler.py": "agent/data_profiler.py",
    "data_analysis_v3/core/data_validator.py": "agent/data_validator.py",
    "data_analysis_v3/core/column_validator.py": "agent/column_validator.py",
    "data_analysis_v3/core/encoding_handler.py": "agent/encoding_handler.py",
    "data_analysis_v3/core/metadata_cache.py": "agent/metadata_cache.py",
    "data_analysis_v3/core/lazy_loader.py": "agent/lazy_loader.py",
    "data_analysis_v3/core/scope_utils.py": "agent/scope_utils.py",
    "data_analysis_v3/core/state.py": "agent/state.py",
    "data_analysis_v3/core/state_manager.py": "agent/state_manager.py",
    "data_analysis_v3/core/analytics_helpers.py": "agent/analytics_helpers.py",
    "data_analysis_v3/core/tpr_language_interface.py": "tpr/language.py",
    "data_analysis_v3/core/tpr_intent_classifier.py": "tpr/intent.py",
    "data_analysis_v3/tools/python_tool.py": "agent/tools/python_tool.py",
    "data_analysis_v3/tools/map_tools.py": "agent/tools/map_tools.py",
    "data_analysis_v3/tools/tpr_analysis_tool.py": "tpr/analysis_tool.py",
    "data_analysis_v3/tools/tpr_workflow_langgraph_tool.py": "tpr/workflow_tool.py",
    "data_analysis_v3/tpr/workflow_manager.py": "tpr/workflow_manager.py",
    "data_analysis_v3/tpr/data_analyzer.py": "tpr/data_analyzer.py",
    "data_analysis_v3/prompts/system_prompt.py": "agent/prompts/system_prompt.py",

    # ── tools/ → visualization/ + planning/ + agent/tools/ ───
    "tools/visualization_maps_tools.py": "visualization/maps_tools.py",
    "tools/variable_distribution.py": "visualization/variable_distribution.py",
    "tools/settlement_visualization_tools.py": "visualization/settlement_tools.py",
    "tools/settlement_intervention_tools.py": "planning/settlement_intervention.py",
    "tools/itn_planning_tools.py": "planning/itn_tools.py",
    "tools/export_tools.py": "planning/export_tools.py",
    "tools/complete_analysis_tools.py": "analysis/complete_tools.py",
    "tools/tpr_query_tool.py": "tpr/query_tool.py",
    "tools/methodology_explanation_tools.py": "agent/tools/methodology_tool.py",
    "tools/chatmrpt_help_tool.py": "agent/tools/help_tool.py",
    "tools/data_description_tools.py": "agent/tools/data_description.py",
    "tools/custom_analysis_parser.py": "agent/tools/analysis_parser.py",
    "tools/base.py": "utils/tool_base.py",

    # ── data/ → services/ ────────────────────────────────────
    "data/unified_dataset_builder.py": "services/dataset_builder.py",
    "data/validation.py": "services/data_validation.py",
    "data/analysis.py": "services/data_analysis.py",
    "data/reporting.py": "services/data_reporting.py",
    "data/flexible_data_access.py": "services/data_access.py",
    "data/processing.py": "services/data_processing.py",
    "data/loaders.py": "services/data_loaders.py",
    "data/utils.py": "services/data_utils.py",
    "data/settlement_loader.py": "services/settlement_loader.py",
    "data/population_data/itn_population_loader.py": "planning/population_loader.py",

    # ── services/agents/visualizations/ → visualization/ ─────
    "services/agents/visualizations/composite_visualizations.py": "visualization/composite.py",
    "services/agents/visualizations/pca_visualizations.py": "visualization/pca.py",
    "services/agents/visualizations/core_utils.py": "visualization/geo_utils.py",
    "services/agents/visualizations/tpr_visualization_service.py": "visualization/tpr_viz.py",

    # ── services/ misc moves ─────────────────────────────────
    "services/universal_viz_explainer.py": "visualization/explainer.py",
    # shapefile_fetcher.py stays in services/ — no move needed
    "services/variable_resolution_service.py": "services/variable_resolver.py",

    # ── helpers/ → utils/ ────────────────────────────────────
    "helpers/error_recovery_helper.py": "utils/error_recovery.py",
    "helpers/tool_discovery_helper.py": "utils/tool_discovery.py",
    "helpers/workflow_progress_helper.py": "utils/workflow_progress.py",
    "helpers/data_requirements_helper.py": "utils/data_requirements.py",
    "helpers/welcome_helper.py": "utils/welcome.py",

    # ── interaction/ → services/ ─────────────────────────────
    "interaction/core.py": "services/interaction_core.py",
    "interaction/events.py": "services/interaction_events.py",
    "interaction/storage.py": "services/interaction_storage.py",
    "interaction/utils.py": "services/interaction_utils.py",

    # ── routing/ → agent/ ────────────────────────────────────
    "routing/semantic_router.py": "agent/semantic_router.py",

    # ── runtime/ → upload/ + services/ ───────────────────────
    "runtime/upload_service.py": "upload/upload_service.py",
    "runtime/standard/workflow.py": "services/standard_workflow.py",

    # ── web/routes/ → api/ ───────────────────────────────────
    "web/routes/core_routes.py": "api/core_routes.py",
    "web/routes/upload_routes.py": "api/upload_routes.py",
    "web/routes/data_analysis_v3_routes.py": "api/data_analysis_routes.py",
    "web/routes/visualization_routes.py": "api/visualization_routes.py",
    "web/routes/export_routes.py": "api/export_routes.py",
    "web/routes/conversation_routes.py": "api/conversation_routes.py",
    "web/routes/arena_routes.py": "api/arena_routes.py",
    "web/routes/session_routes.py": "api/session_routes.py",
    "web/routes/reports_api_routes.py": "api/reports_routes.py",
    "web/routes/debug_routes.py": "api/debug_routes.py",
    "web/routes/itn_routes.py": "api/itn_routes.py",
    "web/routes/api_routes.py": "api/api_routes.py",
    "web/routes/compatibility.py": "api/compatibility.py",
    "web/routes/analysis/chat_stream_service.py": "api/analysis/chat_stream.py",
    "web/routes/analysis/chat_sync_service.py": "api/analysis/chat_sync.py",
    "web/routes/analysis/chat_routing.py": "api/analysis/chat_routing.py",
    "web/routes/analysis/arena_helpers.py": "api/analysis/arena_helpers.py",
    "web/routes/analysis/analysis_chat.py": "api/analysis/analysis_chat.py",
    "web/routes/analysis/analysis_exec.py": "api/analysis/analysis_exec.py",
    "web/routes/analysis/analysis_vote.py": "api/analysis/analysis_vote.py",
    "web/routes/analysis/utils.py": "api/analysis/utils.py",
}

# ═══════════════════════════════════════════════════════════════
# MODULE PATH MAPPING: old import → new import
# Built from FILE_MOVES
# ═══════════════════════════════════════════════════════════════

def build_import_mapping():
    """Convert file moves to import path replacements."""
    mapping = {}
    for old_path, new_path in FILE_MOVES.items():
        # Convert file path to module path
        old_module = "app." + old_path.replace("/", ".").replace(".py", "")
        new_module = "app." + new_path.replace("/", ".").replace(".py", "")
        if old_module != new_module:
            mapping[old_module] = new_module
    return mapping


def build_directory_mapping():
    """Build directory-level import replacements."""
    return {
        # Major directory renames
        "app.core.": "app.",  # Will be handled per-file
        "app.data_analysis_v3.core.": "app.agent.",
        "app.data_analysis_v3.tools.": "app.agent.tools.",
        "app.data_analysis_v3.tpr.": "app.tpr.",
        "app.data_analysis_v3.prompts.": "app.agent.prompts.",
        "app.data_analysis_v3.formatters.": "app.agent.",
        "app.web.routes.analysis.": "app.api.analysis.",
        "app.web.routes.": "app.api.",
        "app.web.": "app.api.",
        "app.helpers.": "app.utils.",
        "app.interaction.": "app.services.",
        "app.routing.": "app.agent.",
        "app.runtime.": "app.",
        "app.data.": "app.services.",
        "app.tools.": "app.",  # distributed
    }


# ═══════════════════════════════════════════════════════════════
# EXECUTION
# ═══════════════════════════════════════════════════════════════

def create_directories(dry_run=True):
    """Create all new directories."""
    new_dirs = set()
    for new_path in FILE_MOVES.values():
        parent = str(Path(new_path).parent)
        if parent != ".":
            new_dirs.add(parent)

    # Add explicit new top-level dirs
    new_dirs.update([
        "agent", "agent/tools", "agent/prompts",
        "api", "api/analysis",
        "arena",
        "conversation",
        "planning",
        "tpr",
        "upload",
        "visualization",
    ])

    for d in sorted(new_dirs):
        full_path = APP_DIR / d
        if dry_run:
            if not full_path.exists():
                print(f"  MKDIR {full_path}")
        else:
            full_path.mkdir(parents=True, exist_ok=True)
            # Create __init__.py if not exists
            init_file = full_path / "__init__.py"
            if not init_file.exists():
                init_file.write_text("")


def move_files(dry_run=True):
    """Move all files using git mv."""
    moved = 0
    for old_path, new_path in sorted(FILE_MOVES.items()):
        old_full = APP_DIR / old_path
        new_full = APP_DIR / new_path

        if not old_full.exists():
            print(f"  SKIP (not found): {old_path}")
            continue

        if old_full.resolve() == new_full.resolve():
            print(f"  SKIP (same location): {old_path}")
            continue

        if dry_run:
            print(f"  {old_path} → {new_path}")
        else:
            new_full.parent.mkdir(parents=True, exist_ok=True)
            result = subprocess.run(["git", "mv", str(old_full), str(new_full)],
                                   capture_output=True, text=True)
            if result.returncode != 0:
                # Fallback: copy + git add + remove old
                print(f"  WARN git mv failed for {old_path}, using copy: {result.stderr.strip()}")
                shutil.copy2(str(old_full), str(new_full))
                subprocess.run(["git", "add", str(new_full)], capture_output=True)
                subprocess.run(["git", "rm", "-f", str(old_full)], capture_output=True)
        moved += 1

    return moved


def update_imports(dry_run=True):
    """Update all import statements in all .py files."""
    import_map = build_import_mapping()

    # Sort by longest path first to avoid partial replacements
    sorted_replacements = sorted(import_map.items(),
                                  key=lambda x: len(x[0]), reverse=True)

    # Find all .py files (excluding archived and pycache)
    py_files = []
    for root, dirs, files in os.walk(APP_DIR):
        dirs[:] = [d for d in dirs if d not in ('__pycache__', '_archived')]
        for f in files:
            if f.endswith('.py'):
                py_files.append(Path(root) / f)

    # Also check top-level files
    for f in ['run.py', 'gunicorn_config.py']:
        p = PROJECT_ROOT / f
        if p.exists():
            py_files.append(p)

    # Check test files too
    tests_dir = PROJECT_ROOT / "tests"
    if tests_dir.exists():
        for root, dirs, files in os.walk(tests_dir):
            dirs[:] = [d for d in dirs if d not in ('__pycache__',)]
            for f in files:
                if f.endswith('.py'):
                    py_files.append(Path(root) / f)

    updated_files = 0
    total_replacements = 0

    for py_file in py_files:
        try:
            content = py_file.read_text(encoding='utf-8')
        except (UnicodeDecodeError, PermissionError):
            continue

        original = content
        file_replacements = 0

        for old_import, new_import in sorted_replacements:
            if old_import in content:
                content = content.replace(old_import, new_import)
                file_replacements += content.count(new_import)

        if content != original:
            if dry_run:
                print(f"  UPDATE {py_file.relative_to(PROJECT_ROOT)} ({file_replacements} replacements)")
            else:
                py_file.write_text(content, encoding='utf-8')
            updated_files += 1
            total_replacements += file_replacements

    return updated_files, total_replacements


def handle_special_cases(dry_run=True):
    """Handle files that need special treatment."""
    specials = []

    # 1. Move data files BEFORE deleting app/data/
    data_pop = APP_DIR / "data" / "population_data"
    if data_pop.exists():
        target = PROJECT_ROOT / "data" / "population_data"
        if dry_run:
            specials.append(f"MOVE DATA: {data_pop} → {target}")
        else:
            target.mkdir(parents=True, exist_ok=True)
            for f in data_pop.iterdir():
                if f.name != "__pycache__" and f.is_file():
                    shutil.copy2(str(f), str(target / f.name))

    # 2. Handle app/data/__init__.py (DataHandler class - 1250 lines)
    data_init = APP_DIR / "data" / "__init__.py"
    if data_init.exists():
        if dry_run:
            specials.append(f"MOVE: data/__init__.py → services/data_handler.py")
        else:
            target = APP_DIR / "services" / "data_handler.py"
            shutil.copy2(str(data_init), str(target))

    # 3. Handle app/tools/__init__.py (tool registry)
    tools_init = APP_DIR / "tools" / "__init__.py"
    if tools_init.exists():
        if dry_run:
            specials.append(f"MOVE: tools/__init__.py → utils/tool_registry.py")
        else:
            target = APP_DIR / "utils" / "tool_registry.py"
            shutil.copy2(str(tools_init), str(target))

    # 4. Handle app/web/routes/__init__.py (blueprint registration)
    routes_init = APP_DIR / "web" / "routes" / "__init__.py"
    if routes_init.exists():
        if dry_run:
            specials.append(f"MOVE: web/routes/__init__.py → api/__init__.py")
        else:
            target = APP_DIR / "api" / "__init__.py"
            shutil.copy2(str(routes_init), str(target))

    # 5. Handle app/interaction/__init__.py (has code)
    interaction_init = APP_DIR / "interaction" / "__init__.py"
    if interaction_init.exists():
        if dry_run:
            specials.append(f"PRESERVE: interaction/__init__.py exports → services/interaction_init.py")
        else:
            target = APP_DIR / "services" / "interaction_init.py"
            shutil.copy2(str(interaction_init), str(target))

    for s in specials:
        print(f"  SPECIAL: {s}")

    return len(specials)


def verify(dry_run=True):
    """Verify all files compile."""
    if dry_run:
        print("  (skipped in dry-run)")
        return True

    errors = []
    for root, dirs, files in os.walk(APP_DIR):
        dirs[:] = [d for d in dirs if d not in ('__pycache__', '_archived')]
        for f in files:
            if f.endswith('.py'):
                path = os.path.join(root, f)
                try:
                    import py_compile
                    py_compile.compile(path, doraise=True)
                except py_compile.PyCompileError as e:
                    errors.append(f"  COMPILE ERROR: {path}: {e}")

    if errors:
        print("\n".join(errors))
        return False

    # Try to import the app
    try:
        result = subprocess.run(
            [sys.executable, "-c", "from app import create_app; print('OK')"],
            capture_output=True, text=True, timeout=30,
            cwd=str(PROJECT_ROOT)
        )
        if result.returncode != 0:
            print(f"  APP IMPORT FAILED: {result.stderr}")
            return False
        print("  App factory imports OK")
    except subprocess.TimeoutExpired:
        print("  APP IMPORT TIMEOUT")
        return False

    return True


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ('--dry-run', '--execute'):
        print("Usage: python migrate_structure.py --dry-run|--execute")
        sys.exit(1)

    dry_run = sys.argv[1] == '--dry-run'
    mode = "DRY RUN" if dry_run else "EXECUTING"

    print(f"\n{'='*60}")
    print(f"ChatMRPT Restructuring — {mode}")
    print(f"{'='*60}\n")

    print(f"[1/6] Creating directories...")
    create_directories(dry_run)

    print(f"\n[2/6] Handling special cases...")
    specials = handle_special_cases(dry_run)
    print(f"  {specials} special cases")

    print(f"\n[3/6] Moving {len(FILE_MOVES)} files...")
    moved = move_files(dry_run)
    print(f"  {moved} files moved")

    print(f"\n[4/6] Updating imports...")
    updated, replacements = update_imports(dry_run)
    print(f"  {updated} files updated, {replacements} import replacements")

    print(f"\n[5/6] Verifying compilation...")
    ok = verify(dry_run)

    print(f"\n[6/6] Summary")
    print(f"  Files moved: {moved}")
    print(f"  Imports updated: {updated} files")
    print(f"  Special cases: {specials}")
    print(f"  Compilation: {'OK' if ok else 'ERRORS'}")

    if not dry_run and ok:
        print(f"\n  Ready to commit. Run:")
        print(f"  git add -A && git commit -m 'refactor: restructure codebase to user-journey directories'")

    print()


if __name__ == "__main__":
    main()
