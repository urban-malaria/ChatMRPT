"""Add question text, answer text and answer keys to the study participants' pre/post survey answers.

Run after scripts/prepost_study_participants.py. Question wording, options and answer keys come from
app/prepost/questions.py, the survey as served by ChatMRPT. The background and concept questions, their
options and keys are the same in every saved version of the code from 21 October 2025 (the first training)
onward; the usability questions were added later and appear only in post-tests.

Multiple-choice answers are stored as option numbers starting at 0; they are converted to the option text.
A multiple-choice answer is correct when it matches the key exactly (for "choose all that apply", the same
set of options, as in PrePostQuestions.calculate_score). Open-ended questions have no answer key.

Outputs (data/chatmrpt_prepost_evaluation/study_participants_2026-11-03):
  prepost_analysis.xlsx        Read me; Answers (one row per participant, test and question)
  answers_with_key_long.csv    the Answers sheet as CSV
"""
import ast
import importlib.util
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "data/chatmrpt_prepost_evaluation/study_participants_2026-11-03"
spec = importlib.util.spec_from_file_location("questions", ROOT / "app/prepost/questions.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
Q = module.PrePostQuestions
QUESTIONS = [q for q in Q.BACKGROUND_QUESTIONS + Q.CONCEPT_QUESTIONS + Q.USABILITY_QUESTIONS if q["type"] != "section_header"]
BY_ID = {q["id"]: q for q in QUESTIONS}
ORDER = {q["id"]: i for i, q in enumerate(QUESTIONS)}


def key_indices(q):
    if "correct_index" in q:
        return [q["correct_index"]]
    return list(q.get("correct_indices", []))


def option_text(q, indices):
    return "; ".join(q["options"][i] for i in indices)


def parse(q, raw):
    """Selected option numbers for a multiple-choice answer, or None if it cannot be read."""
    if pd.isna(raw):
        return None
    value = ast.literal_eval(raw) if q["type"] == "checkbox" else int(raw)
    selected = value if isinstance(value, list) else [value]
    assert all(0 <= i < len(q["options"]) for i in selected), (q["id"], raw)
    return sorted(selected)


long = pd.read_csv(DIR / "study_participants_long_latest_answer.csv", dtype=str)
check = pd.read_csv(DIR / "participant_check.csv", dtype=str).set_index("phone_last4")
long["cohort"] = long.phone_last4.map(check.cohort)
assert set(long.question_id) <= set(BY_ID), set(long.question_id) - set(BY_ID)
rows = []
for r in long.itertuples():
    q = BY_ID[r.question_id]
    out = {"phone_last4": r.phone_last4, "cohort": r.cohort, "test": r.test_type, "section": q["section"],
           "question_id": q["id"], "question": q["text"], "answer": r.response, "answer_key": "", "correct": ""}
    if q["type"] in ("radio", "checkbox"):
        selected = parse(q, r.response)
        out["answer"] = option_text(q, selected) if selected is not None else ""
        if key_indices(q):
            out["answer_key"] = option_text(q, key_indices(q))
            out["correct"] = "" if selected is None else ("Yes" if selected == sorted(key_indices(q)) else "No")
    elif q["type"] == "scale" and str(r.response).strip().isdigit() and 1 <= int(r.response) <= 5:
        out["answer"] = f"{r.response} ({q['labels'][int(r.response) - 1]})"
    rows.append(out)
answers = pd.DataFrame(rows)
answers = answers.sort_values(["cohort", "phone_last4", "test", "question_id"],
                              key=lambda s: s.map(ORDER) if s.name == "question_id" else s)
answers.to_csv(DIR / "answers_with_key_long.csv", index=False)

keyed = answers[answers.answer_key.ne("")]
people = check.index[check.included.eq("Yes")]
tests = answers[answers.section.eq("concepts")].groupby("phone_last4").test.agg(set)
n_both = int(tests.reindex(people).map(lambda t: isinstance(t, set) and {"pre", "post"} <= t).sum())
late = check.loc[people].note.dropna()
late = late[late.str.startswith("Pre-test taken")]
readme = pd.DataFrame({"Item": ["What this is", "Questions and answer keys", "Correct", "Participants", "Notes"],
                       "Description": [
    "Pre- and post-test answers of the ChatMRPT study participants, one row per participant, test and question, "
    "with the question wording, the answer key and whether the answer is correct.",
    f"From the survey as served in ChatMRPT (app/prepost/questions.py), the same in every saved version of the code "
    f"from 21 October 2025 onward. {keyed.question_id.nunique()} concept questions have an answer key; the others are "
    "open-ended or background questions and have no key.",
    "Yes if the answer matches the key. For 'choose all that apply' questions, all correct options and no others "
    "must be chosen, as in ChatMRPT's own scoring.",
    f"{len(people)} study participants; {n_both} took both the pre- and post-test.",
    "Where a question was answered more than once because of connection problems, the latest answer is used. "
    "Pre-test answers given after a participant's first post-test answer are excluded. "
    + (f"{', '.join(late.index)} took the pre-test after their group's first training day." if len(late) else "")]})
with pd.ExcelWriter(DIR / "prepost_analysis.xlsx", engine="openpyxl") as xl:
    readme.to_excel(xl, sheet_name="Read me", index=False)
    answers.to_excel(xl, sheet_name="Answers", index=False)
    for ws in xl.book.worksheets:
        for col in ws.columns:
            width = max(len(str(c.value)) if c.value is not None else 0 for c in col)
            ws.column_dimensions[col[0].column_letter].width = min(max(10, width + 2), 80 if ws.title == "Answers" else 120)

print(len(answers), "rows;", len(people), "participants;", n_both, "with both tests")
print(keyed.groupby(["cohort", "test", "question_id"]).correct.apply(lambda s: f"{(s == 'Yes').sum()}/{s.ne('').sum()}").unstack("test").to_string())
