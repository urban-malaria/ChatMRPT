"""Knowledge library: entries parse, retrieval finds the right topic."""

import re

import pytest

from app.knowledge import library
from app.knowledge.library import context_for, core_brief, find, load_entries, parse_entry

BANNED_PHRASES = ["transmission potential", "forecasting", "we forecast", "predicts future"]


def test_all_entries_load_with_unique_ids():
    entries = load_entries()
    assert len(entries) >= 15
    for entry in entries.values():
        assert entry.title and entry.summary and entry.status in library.VALID_STATUSES


def test_prompt_entries_have_aliases():
    for entry in load_entries().values():
        if entry.in_prompts:
            assert entry.aliases, f"{entry.id} has no aliases"


def test_approved_entries_with_numbers_have_sources():
    for entry in load_entries().values():
        if entry.status == "approved" and re.search(r"\d+\s*%", entry.body + entry.summary):
            assert entry.sources, f"{entry.id} states percentages without sources"


@pytest.mark.parametrize("question, expected", [
    ("How does ChatMRPT work?", "about-chatmrpt"),
    ("What is TPR?", "tpr"),
    ("What is the malaria burden per 1,000?", "malaria-burden"),
    ("How is the composite score calculated?", "composite-scoring"),
    ("Why was PCA skipped?", "pca"),
    ("What does High Risk mean?", "risk-categories"),
    ("Which variables are used for my zone?", "risk-variables"),
    ("How do you allocate bed nets?", "itn-planning"),
    ("What data do I need to upload?", "data-requirements"),
    ("How do I classify informal settlements?", "settlement-classification"),
    ("What is U5?", "glossary"),
    ("What is Arena?", "arena"),
])
def test_find_returns_expected_topic_first(question, expected):
    found = find(question)
    assert found and found[0].id == expected, [e.id for e in found]


def test_find_ignores_partial_words():
    assert not any(e.id == "tpr" for e in find("I want to output a table"))


def test_hold_entries_never_reach_prompts():
    held = [e for e in load_entries().values() if e.status == "hold"]
    assert held
    brief = core_brief()
    for entry in held:
        assert entry.title not in brief
        assert entry not in find(" ".join(entry.aliases) or entry.title)


@pytest.mark.parametrize("question", ["How does ChatMRPT work?", "What is TPR?", "hello"])
def test_prompt_text_has_no_banned_claims(question):
    text = context_for(question).lower()
    for phrase in BANNED_PHRASES:
        assert phrase not in text


def test_context_includes_rule_and_details():
    text = context_for("How does ChatMRPT work?")
    assert library.NOT_COVERED_RULE in text
    assert "What it does not do" in text


def test_parse_entry_rejects_bad_files():
    with pytest.raises(ValueError):
        parse_entry("no front matter")
    with pytest.raises(ValueError):
        parse_entry("---\nid: x\ntitle: X\nsummary: s\nstatus: maybe\n---\nbody")
    with pytest.raises(ValueError):
        parse_entry("---\nid: x\ntitle: X\n---\nbody")
