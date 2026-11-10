"""Filter the ChatMRPT pre/post survey to the study participants.

Inputs (data/chatmrpt_prepost_evaluation/raw, not committed):
  pretest_2_18_states_participants.xlsx  participant list for pretest 2 from Laurette Mhlanga (Groups A and B)
  prepost_survey_all_sessions_2025-10-01_to_2026-05-19.csv  survey export, long format

Two training cohorts:
  Pretest 1, October 2025: pre-tests on 21 October 2025, post-tests on 4-5 November 2025. There is no
    participant list, so participants are the phone numbers that started a pre-test on 21 October.
  Pretest 2, January-February 2026: Group A from 26 January and Group B from 2 February 2026. Participants
    are matched to the list on the last four digits of their phone number; sessions from 26 January to
    9 February 2026 are kept, and phone numbers in that window that are not on the list are flagged.
Sessions from 1 to 20 October 2025 (app testing, e.g. 1111, 2222) and the Kwara training of May 2026 are
not part of either cohort and are excluded.

Pre-test answers given after the participant's first post-test answer are dropped, so the pre-test is a
measure taken before the post-test. Where a question was then answered more than once in the same test
(connection problems), the latest answer is kept and the number of answers is recorded. Pre-tests taken
after the group's first training day are flagged.

Included: pretest 2 participants on the list, and pretest 1 participants (no list; identified by the
21 October pre-test), excluding a pretest 1 entry whose job title is a postdoctoral researcher (study team,
not a programme officer). Pretest 2 numbers not on the list are left out. Everyone left out is listed in
participant_check.csv with the reason.

Outputs (data/chatmrpt_prepost_evaluation/study_participants_2026-11-03):
  study_participants_wide.csv              one row per confirmed participant
  study_participants_long_latest_answer.csv one row per confirmed participant, test and question
  participant_check.csv                    every candidate: cohort, list match, included or not, tests taken, repeated answers
"""
import re
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data/chatmrpt_prepost_evaluation"
RAW = DATA / "raw"
OUT = DATA / "study_participants_2026-11-03"
OUT.mkdir(parents=True, exist_ok=True)
GROUP_A, GROUP_B = "Pretest 2, Group A (from 26 Jan 2026)", "Pretest 2, Group B (from 2 Feb 2026)"

def last4(v):
    if pd.isna(v): return None
    s = re.sub(r"\D", "", str(v).split(".")[0])
    return s[-4:] if len(s) >= 4 else None

x = pd.ExcelFile(RAW / "pretest_2_18_states_participants.xlsx")
groups = []
for sheet, label in [("Group A ", GROUP_A), ("Group B", GROUP_B)]:
    g = x.parse(sheet)[["State", "Phone Number"]].copy()
    g["phone_last4"] = g["Phone Number"].map(last4); g["listed_group"] = label
    groups.append(g.drop(columns="Phone Number"))
listed = pd.concat(groups, ignore_index=True)

a = pd.read_csv(RAW / "prepost_survey_all_sessions_2025-10-01_to_2026-05-19.csv", dtype={"phone_last4": str})
a["phone_last4"] = a.phone_last4.str.zfill(4)
started = pd.to_datetime(a.session_started_at)

pretest1_ids = set(a.loc[started.dt.normalize().eq("2025-10-21") & a.test_type.eq("pre"), "phone_last4"])
pretest1 = a[a.phone_last4.isin(pretest1_ids) & (started >= "2025-10-21") & (started < "2025-11-06")].assign(
    cohort="Pretest 1 (Oct 2025)")
pretest2 = a[(started >= "2026-01-26") & (started < "2026-02-10")].assign(cohort="Pretest 2 (Jan-Feb 2026)")
assert not set(pretest1.phone_last4) & set(pretest2.phone_last4), "A phone number appears in both cohorts."
study = pd.concat([pretest1, pretest2], ignore_index=True)

ids = study.groupby("phone_last4").agg(cohort=("cohort", "first"), first_session=("session_started_at", "min"),
                                       tests_taken=("test_type", lambda t: ", ".join(sorted(set(t)))),
                                       answers=("response", "size")).reset_index()
ids["first_session"] = ids.first_session.str[:10]
ids = ids.merge(listed, on="phone_last4", how="left")
is_p2 = ids.cohort.str.startswith("Pretest 2")
ids["group"] = ids.listed_group
ids.loc[is_p2 & ids.group.isna(), "group"] = ids.first_session.map(
    lambda d: GROUP_A if d < "2026-02-02" else GROUP_B) + " - inferred from date"
ids.loc[~is_p2, "group"] = "Pretest 1 (Oct 2025)"
ids["on_participant_list"] = ids.listed_group.notna().map({True: "Yes", False: "No"})
ids.loc[~is_p2, "on_participant_list"] = "No list for this cohort"
ids["note"] = ""
ids.loc[is_p2 & ids.listed_group.isna(), "note"] = "Took the survey during the training but phone not on the list"
job = a[a.question_id.eq("bg_job_title")].groupby("phone_last4").response.last()
ids["job_title"] = ids.phone_last4.map(job)
study_team = ~is_p2 & ids.job_title.fillna("").str.contains(r"post\s*-?\s*doc", case=False)
ids["included"] = "Yes"
ids.loc[is_p2 & ids.on_participant_list.ne("Yes"), "included"] = "No, not on the participant list"
ids.loc[study_team, "included"] = "No, study team (job title: postdoctoral researcher)"
answered = study.assign(day=study.response_timestamp.str[:10]).groupby("phone_last4").day.agg(["min", "max"])
ids.loc[study_team, "note"] = ("Session opened 21 Oct 2025; answers given " + ids.phone_last4.map(answered["min"]) +
                               " to " + ids.phone_last4.map(answered["max"]))
ids = ids.drop(columns="listed_group").sort_values(["cohort", "group", "phone_last4"])

candidates = ids
ids = ids[ids.included.eq("Yes")]
study = study[study.phone_last4.isin(ids.phone_last4)].copy()
study["response_timestamp"] = pd.to_datetime(study.response_timestamp)
study = study.sort_values("response_timestamp")
first_post = study[study.test_type.eq("post")].groupby("phone_last4").response_timestamp.min()
late_pre = study.test_type.eq("pre") & (study.response_timestamp > study.phone_last4.map(first_post))
late_pre_counts = study[late_pre].groupby("phone_last4").size()
study = study[~late_pre]
dates = study.groupby(["phone_last4", "test_type"]).response_timestamp.min().dt.strftime("%Y-%m-%d").unstack()
ids = ids.assign(pre_test_date=ids.phone_last4.map(dates.get("pre")), post_test_date=ids.phone_last4.map(dates.get("post")),
                 pre_answers_after_post_dropped=ids.phone_last4.map(late_pre_counts).fillna(0).astype(int))
first_day = ids.group.map({GROUP_A: "2026-01-26", GROUP_B: "2026-02-02", "Pretest 1 (Oct 2025)": "2025-10-21"})
late = ids.pre_test_date.notna() & (ids.pre_test_date > first_day)
ids.loc[late, "note"] = "Pre-test taken on " + ids.pre_test_date + ", after the group's first training day"
key = ["phone_last4", "test_type", "question_id"]
counts = study.groupby(key).response.agg(times_answered="size", different_answers="nunique").reset_index()
latest = study.drop_duplicates(key, keep="last").merge(counts, on=key)
latest = latest.merge(ids[["phone_last4", "group", "State"]], on="phone_last4", how="left")
latest.to_csv(OUT / "study_participants_long_latest_answer.csv", index=False)

bg = latest[latest.section.eq("background") & latest.test_type.eq("pre")].pivot(index="phone_last4", columns="question_id", values="response")
qa = latest[~latest.section.eq("background")].assign(col=lambda d: d.question_id + "_" + d.test_type)
qa = qa.pivot(index="phone_last4", columns="col", values="response")
cols = ["cohort", "group", "State", "tests_taken", "pre_test_date", "post_test_date", "note"]
wide = ids.set_index("phone_last4")[cols].join(bg).join(qa.reindex(sorted(qa.columns), axis=1))
wide.to_csv(OUT / "study_participants_wide.csv")

dups = counts[counts.times_answered > 1]
summary = dups.groupby("phone_last4").agg(questions_answered_more_than_once=("question_id", "size"),
                                           with_different_answers=("different_answers", lambda s: int((s > 1).sum())))
candidates.assign(note=candidates.phone_last4.map(ids.set_index("phone_last4").note).fillna(candidates.note)).set_index("phone_last4") \
   .join(ids.set_index("phone_last4")[["pre_test_date", "post_test_date", "pre_answers_after_post_dropped"]]).join(summary).fillna({"questions_answered_more_than_once": 0, "with_different_answers": 0}) \
   .to_csv(OUT / "participant_check.csv")

print(candidates.groupby(["cohort", "included"]).size().to_string())
print(len(wide), "rows wide;", len(latest), "rows long; pre-test answers given after the post-test dropped:", late_pre_counts.to_dict())
print(ids[ids.note.ne("")][["phone_last4", "group", "pre_test_date", "post_test_date", "note"]].to_string(index=False))
print("repeat answers:", len(dups), "participant-question pairs;", int((dups.different_answers > 1).sum()), "differ")
print(candidates[candidates.included.ne("Yes")][["phone_last4", "cohort", "group", "tests_taken", "answers", "note"]].to_string(index=False))
print("Listed but not found:", sorted(set(listed.phone_last4.dropna()) - set(candidates.phone_last4)),
      "| listed without a phone:", listed[listed.phone_last4.isna()].State.tolist())
excluded = a[~a.session_id.isin(study.session_id)]
print("Excluded sessions:", excluded.groupby(excluded.session_started_at.str[:7]).phone_last4.agg(lambda s: sorted(set(s))).to_dict())
