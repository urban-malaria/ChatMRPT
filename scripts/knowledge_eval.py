"""Check that every chat model answers ChatMRPT questions correctly.

Sends a fixed set of demo questions to the main chat model and each Arena
model, using the same system prompts the app builds from the knowledge
library, and checks each answer for required facts and known wrong claims.
Writes a Markdown report. Uses real API calls; run manually before demos.

Usage:
    venv/bin/python scripts/knowledge_eval.py
    venv/bin/python scripts/knowledge_eval.py --targets chat --extra-questions prof_questions.txt
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from app.agent.prompt_builder import PromptBuilder  # noqa: E402
from app.arena.prompts import get_arena_system_prompt  # noqa: E402
from app.config.arena import ARENA_MODELS  # noqa: E402
from app.services.llm_adapter import LLMAdapter  # noqa: E402

GLOBAL_BANNED = ["transmission potential"]
NEGATION = re.compile(r"\b(not|no|never|cannot|without)\b|n't\b")


@dataclass
class Case:
    question: str
    required: Sequence[Sequence[str]] = ()   # every group must have at least one match
    banned: Sequence[str] = field(default_factory=list)


CASES: List[Case] = [
    Case("How does ChatMRPT work?", [["burden"], ["rank"], ["upload"]], ["forecast"]),
    Case("What is TPR?", [["positive"], ["tested", "tests"]]),
    Case("What is malaria burden in ChatMRPT?", [["1,000", "1000"], ["population"]]),
    Case("How does ChatMRPT calculate the composite score?", [["median"], ["0", "rescal", "normali"]]),
    Case("Why might PCA be skipped in ChatMRPT?", [["kmo", "kaiser"], ["bartlett"]]),
    Case("What does High Risk mean in ChatMRPT?", [["third"]]),
    Case("What data do I need to upload to ChatMRPT?", [["tested"], ["positive"]]),
    Case("How does ChatMRPT allocate bed nets?", [["1.8"], ["rural"]]),
    Case("What is settlement classification in ChatMRPT?", [["formal"], ["informal"], ["slum"]], ["automatically classif"]),
    Case("Can ChatMRPT forecast next year's malaria cases?", [["not", "no ", "cannot", "can't"]]),
    Case("Which variables does ChatMRPT use for risk analysis?", [["rainfall", "ndwi", "elevation", "housing"]]),
    Case("What is the TPR workflow in ChatMRPT?", [["facility"], ["age"]]),
    Case("Does ChatMRPT give treatment advice?", [["not", "no "]]),
    Case("What is Arena in ChatMRPT?", [["model"]]),
    Case("How does ChatMRPT use urban percentage?", [["threshold"]]),
]


def check(case: Case, answer: str) -> Tuple[bool, List[str]]:
    text = answer.lower()
    problems = []
    if not answer.strip() or text.startswith(("error:", "openai error", "groq error")):
        return False, [f"no answer: {answer[:120]}"]
    for group in case.required:
        if not any(term.lower() in text for term in group):
            problems.append(f"missing one of {list(group)}")
    sentences = re.split(r"(?<=[.!?])\s+|\n+", text)
    for phrase in list(case.banned) + GLOBAL_BANNED:
        claims = [s for s in sentences if phrase.lower() in s and not NEGATION.search(s)]
        if claims:
            problems.append(f"claims '{phrase}': {claims[0][:150]}")
    return not problems, problems


def ask(target: str, question: str) -> str:
    if target == "chat":
        system = PromptBuilder().build({}, None, question)
        adapter = LLMAdapter(backend="openai", model="gpt-4o")
    else:
        system = get_arena_system_prompt(question)
        adapter = LLMAdapter(backend=ARENA_MODELS[target].get("provider", "groq"), model=target)
    answer = ""
    for attempt in range(5):
        try:
            answer = adapter.generate(prompt=question, max_tokens=900, temperature=0.2, system_message=system)
        except Exception as exc:  # report, don't abort the run
            answer = f"Error: {exc}"
        if "rate limit" not in answer.lower():
            break
        time.sleep(20 * (attempt + 1))
    return answer


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--targets", nargs="*", default=["chat", *ARENA_MODELS.keys()],
                        help="'chat' and/or Arena model ids")
    parser.add_argument("--extra-questions", type=Path,
                        help="text file, one question per line (reported, not auto-checked beyond banned phrases)")
    parser.add_argument("--out", type=Path,
                        default=ROOT / "project" / "planning" / f"knowledge_eval_{datetime.now():%Y-%m-%d}.md")
    args = parser.parse_args()

    cases = list(CASES)
    if args.extra_questions:
        cases += [Case(q.strip()) for q in args.extra_questions.read_text().splitlines() if q.strip()]

    # One thread per model, questions in sequence, to stay under provider rate limits.
    def run_target(target: str) -> List[str]:
        return [ask(target, c.question) for c in cases]

    with ThreadPoolExecutor(max_workers=len(args.targets)) as pool:
        per_target = list(pool.map(run_target, args.targets))
    jobs = [(t, c) for t in args.targets for c in cases]
    answers = [a for target_answers in per_target for a in target_answers]

    lines = [f"# Knowledge eval {datetime.now():%Y-%m-%d %H:%M}", ""]
    summary = {}
    for (target, case), answer in zip(jobs, answers):
        ok, problems = check(case, answer)
        summary.setdefault(target, [0, 0])[0 if ok else 1] += 1
        lines += [f"## {'PASS' if ok else 'FAIL'} | {target} | {case.question}"]
        lines += [f"- {p}" for p in problems]
        lines += ["", "```", answer.strip()[:2500], "```", ""]
    table = ["| Target | Pass | Fail |", "|---|---|---|"]
    table += [f"| {t} | {p} | {f} |" for t, (p, f) in summary.items()]
    lines[2:2] = table + [""]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(table))
    print(f"Report: {args.out}")
    return 0 if all(f == 0 for _, f in summary.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
