# ChatMRPT usability fix plan, 24 Nov 2026

This plan fixes every problem found in the first-time-user review (`ux_review_2026-11-24/REPORT.md`, problems P1–P15). It also sets the rule for when Arena mode should appear.

**Status:** draft for review. Nothing in it has been started.

**Labels used below**
- Each cause is marked **verified** (checked in the code, logs or data) or **to confirm** (likely, not yet checked).
- Size is **S** (under 2 h), **M** (half a day) or **L** (1–2 days).

---

## Progress
**Phase 1: done and deployed, 24 Nov 2026** (`main` at b83dedb, both servers).

Planned items:
- 1.1 Bed-net downloads have nets per ward.
- 1.2 Ranking and ward questions are answered from the data; a grounding check is added.
- 1.3 Explain uses the chart's own data.
- 1.4 Wards without data show as "no data"; ward matching is corrected.
- 1.5 TPR is pooled correctly.

Added during Phase 1:
- **Malaria burden in the risk score.** The malaria variable had been silently dropped; burden now fills it (decision 24 Nov). Estimated burdens are flagged.
- **Per-year runs** no longer overwrite or corrupt the all-years files.
- **Corrupted names** where "NA" had become "0" (e.g. "Gwa0be") are matched.
- **`libpysal`** installed on both servers: 4.14.1 on server 1 (Python 3.11) and 4.8.1 on server 2 (Python 3.9). Before this, missing values were always filled with the state average.

Noted, not fixed: the two servers run different Python, numpy and scipy versions (3.11 vs 3.9), so results could differ slightly between them.

**Phase 2: done and deployed, 24 Nov 2026** (`main` at 063a986, both servers).

Planned items:
- 2.1 Arena only for general malaria questions; help questions, greetings and the first message get one answer.
- 2.2 Arena explained, with a Skip option, the correct round count and a closing message.
- 2.3 A getting-started entry; short help answers; the KMO block replaced with one sentence.
- 2.4 Sidebar icons labelled; a sample button in the input bar.
- 2.5 Tables, LaTeX, embedded download links, failed charts and chart titles fixed.

Added during Phase 2:
- The chat is locked while the sample loads.
- Asking for the risk analysis before the workflow explains the next step.

## How we will work
- **Order:** one item at a time, in the phases below.
- **Testing:** every item gets the check in its "Test" line. Each phase ends with a full run through the sample-data journey, locally against production data, using the same scripted checks as the 24 Nov test. Before deploying, the phase is run on a copy on a production server.
- **Deploying:**
  - Each finished phase is one branch, merged to `main` and deployed to both servers after your OK.
  - Production is not touched between phases.
- **After Phase 4:** the independent reviewer repeats the newcomer journey, so we can see what changed.

---

## Phase 1: Wrong or invented results (blockers)
A demo audience must never see a wrong number or an invented answer.

### 1.1 Bed-net downloads have no nets per ward (P1)
- **Cause (verified):** `app/planning/export_tools.py` merges the allocation into the CSV only if the stored ITN results have columns `ward_name` and `population`. The ITN step saves `WardName` and `Population` instead, so the merge is silently skipped. Without those columns, the dashboard shows N/A, "People Protected 0" and "Coverage 23" with no %.
- **Fix:**
  - Merge on the real column names, normalising case.
  - The CSV gets Ward, LGA, risk rank, risk category, population, nets needed, nets allocated, coverage % and phase.
  - The dashboard reads the same table.
  - The ZIP summary lists only files that are in the ZIP.
- **Test:** a unit test asserts the CSV has the allocation columns and that the nets add up to the total allocated. Then check the downloaded CSV and dashboard by eye.
- **Size:** M.

### 1.2 The assistant states data it never looked at (P2)
- **Cause (verified, from the server log):** the app suggests "show me the lowest risk wards". When asked, the model answered from the conversation without calling any tool. It invented ward names and scores (0.069, 0.070, … in 0.001 steps). The conversation had also been cut to the last 20 messages.
- **Fix (two layers):**
  1. **Rankings come from the table, not the model.** "Highest / lowest risk wards", "top N" and "where does ward X rank" are answered in code from `analysis_vulnerability_rankings.csv`. The answer always includes LGA, score and category.
  2. **A guard on every data answer.** If a reply quotes ward names or numbers but no tool ran in that turn, it is retried once with a tool call required. If the retry also fails, it says it could not check the data instead of answering.
  - The system prompt also states: never state a number or ward name you have not computed in this turn.
- **Test:** the questions from the review, asked in a fresh chat and in a reopened chat ("lowest risk wards", "which areas are worst?", "why is Owode/Gbogun high risk?"). Every ward named must exist and match the ranking file.
- **Size:** M.

### 1.3 The Explain button invents problems (P2)
- **Cause (verified):** `app/visualization/explainer.py` explains a chart from the session's data files. For a chart type it doesn't know (the workflow's age and facility charts), it falls back to the largest CSV. It then counts 6,948 facility-month rows as "wards", finds no LGA column ("0 LGAs"), and the language model calls this "a data integration error".
- **Fix:**
  - For charts without a known data file, explain from the figure's own data (the numbers inside the chart file).
  - Never call rows "wards" unless the table has a ward column.
  - Tell the model not to diagnose data errors.
- **Test:** click Explain on each chart type the journey produces; no false claims.
- **Size:** M.

### 1.4 Burden numbers contradict each other, and unmatched wards show as zero (P3)
**Causes (verified):**
- The headline adds six years of cases (2020–2025) and divides by one year's population:
  - 574 is the average over the 178 matched wards;
  - 490 is the same average over all 193 map wards, with the unmatched wards counted as 0;
  - 133–174 are the correct per-year values.
- 54 wards hit the 1,000 cap only because six years of cases are added together.
- 24 map wards have no matching DHIS2 ward name. They get burden 0 and population 0, so they look like the safest wards on the map. Gwanara is one of them, and it is still in the top 10 by risk, driven by the environmental variables.

**Fix:**
- **Headline:** see Decision 1. My recommendation is the **average annual burden** (the mean of the yearly values), with the year table beneath it. The same figure goes into the map, the risk analysis and the ITN plan.
- **Wards without data:** "no data", not 0. They show grey on maps, are left out of averages, and are listed by name: "24 wards could not be matched to the DHIS2 data".
- **Ward matching:** improve it, and report which DHIS2 wards were dropped (8 of 186 here).
- **Impossible values:** values above 1,000 per 1,000 in a single year are flagged in the chat and on the map, not presented as normal.

**Test:** headline, map, chat summary and downloads show the same figure. Unmatched wards show grey and are not zero in any CSV. Rerun on Kwara and one other state file.

**Size:** L. This changes the analysis, so it needs your sign-off on the definition.

### 1.5 Data summary figures disagree (P3)
- **Cause (to confirm):** the dataset summary gives "average TPR 23.7" next to an average of 72.9 positives per 105.3 tests (about 69%).
- **Fix:** compute TPR as total positives ÷ total tests.
- **Test:** the summary matches a manual calculation.
- **Size:** S.

---

## Phase 2: First contact and help answers
Someone with no data and no training must know what to do within the first minute.

### 2.1 When Arena appears (P4)
See the proposed policy in **Decision 2** below. The code change:
- Questions about ChatMRPT itself, about the user's data or results, and help or navigation questions always get **one answer**.
- Only general malaria and public-health questions go to Arena.
- The router (`app/api/analysis/chat_routing.py` and `arena_helpers.is_arena_eligible_message`) uses the knowledge library's topic match to decide.

**Cause (verified):** today, with no data loaded, every message of 3+ words goes to Arena, including "how does this work?" and "I don't have any data".

**Size:** M.

### 2.2 Make Arena understandable when it does appear (P4)
- A one-line banner: "You're helping us compare AI models. Read both answers and pick the better one, or skip."
- A **Skip / just answer** button that gives one answer.
- A consistent round count. It now shows "Round 1 of 3" while the indicator shows 2 rounds, because 4 models make 3 rounds.
- A closing message after the last vote.
- Answers capped at about 200 words.
- **Test:** ask a general malaria question and go through all rounds.
- **Size:** M.

### 2.3 Help answers lead with the sample data and drop the jargon (P5)
- **Cause (verified):**
  - The knowledge library says the sample is "in the sidebar" and has no entry for "I don't have data" or "how do I start".
  - The help answers include KMO/Bartlett/PCA detail by default.
- **Fix:**
  - A "Getting started" knowledge entry ("no data? click Try Sample Data", what the sample is, the first thing to type), matched by those questions.
  - Help answers kept to about 6 short lines in plain words.
  - Technical detail moves behind "Ask me for the technical details".
  - The "Behind the Scenes: KMO / Bartlett" block in the risk result is replaced by one plain sentence, with the statistics available on request.
- **Test:** the four first-contact questions from the review give short, correct answers that mention the sample button.
- **Size:** M.

### 2.4 Keep the sample data visible (P5)
- **Cause (verified):** the Try Sample Data button is only on the empty welcome screen. Once a chat starts, the only route is an unlabelled sidebar icon (6 minutes to find in the review).
- **Fix:**
  - Label the sidebar icons ("Chats", "Sample data") on hover and in the expanded rail.
  - Add "Try sample data" next to the upload button in the input bar while no data is loaded.
- **Test:** start chatting first, then find the sample within seconds.
- **Size:** S.

### 2.5 Raw markdown, LaTeX and broken embeds (P6)
**Causes (verified):**
- The Arena panel renders answers without the table plugin the main chat uses, so tables show as raw `|` pipes.
- Models write LaTeX (`\[ \frac{…} \]`) and `<br>` inside tables.
- The page scans message text for anything ending in `.html` and embeds it as a chart. A download link such as `/export/download/…/itn_distribution_dashboard.html` is therefore embedded as `/serve_viz_file/export/download/…`, which returns 404 and shows the raw error JSON `{"message": "Visualization file not found"}`.
- Chart captions are file names ("Burden Multi Year Distribution 20261124 100241").

**Fix:**
- Use the table plugin in the Arena panel.
- Tell all models not to use LaTeX or HTML, and convert `<br>` in tables to line breaks.
- Never embed links that sit inside `/export/` or a markdown link.
- If an embedded chart fails to load, show "This chart could not be loaded" instead of JSON.
- Give every chart a readable title ("Malaria burden by ward, 2020–2025").

**Test:** the review's questions plus the ITN step; no raw syntax, no JSON, readable captions.

**Size:** M.

---

## Phase 3: Following the workflow
The user should always know where they are, why an option is recommended, and that results match their choices.

### 3.1 Changing one choice silently changes another (P7)
- **Cause (verified):** the switch tool's defaults are `facility_level="all"` and `age_group="all_ages"`. "Switch to pregnant women" therefore also resets primary to all.
- **Fix:**
  - Unspecified choices keep their current value.
  - Every result states the active selection ("Primary facilities, under 5s").
  - After a switch, earlier risk rankings and ITN plans are marked out of date, and the reply says "rerun the risk analysis to update the ranking".
- **Test:** switch the age group only, and check the facility level is unchanged and the out-of-date note appears.
- **Size:** M.

### 3.2 Refresh and reopened chats (P8)
- **Cause, missing charts (verified):** the main streaming path saves assistant replies without their charts and downloads (`app/api/data_analysis_routes.py`, around line 524). The non-streaming path does save them.
- **Cause, blank screen after refresh (to confirm):** the chat is stored per conversation ID in browser storage, but the page starts a new conversation on reload.
- **Fix:**
  - Save charts and downloads with every reply.
  - Reopen the last conversation on refresh.
  - Name chats after their content ("Kwara sample – burden, risk, ITN") instead of "New conversation".
- **Test:** do the full journey, refresh, and check everything comes back. Reopen from history and check all charts.
- **Size:** M.

### 3.3 Reasons for recommendations, clickable choices, useful charts (P9)
- **Fix:**
  - Each "Recommended" option gets a one-line reason. For example, primary facilities are where most people are tested (93% of tests here); under-5s are the group most at risk of severe malaria and the usual planning group.
  - Choices become buttons; typing still works. The chat already has a component for clickable options, which the workflow would reuse.
  - Replace the six near-empty histograms with one bar chart comparing tests and positivity across the options.
- **Test:** choose every step by clicking only.
- **Size:** L.

### 3.4 Show the risk result when it is produced (P10)
- **Fix:**
  - The risk-analysis reply names the factors it used, shows the top 10 wards with their LGA, and opens the risk map.
  - The variables listed at the burden step must match those actually used. Today the burden step lists rainfall, NDVI, housing, elevation and urban, while the analysis used a different six.
  - Every ward list includes the LGA.
- **Test:** the result shows the named factors, a table and a map. The factor names match the output file.
- **Size:** M.

### 3.5 Clean names (P12)
- **Cause (verified):** DHIS2 names keep their org-unit prefix and suffix: "kw Kwara State", "kw Moro Local Government Area". The README then shows "kw Kwara State State".
- **Fix:** strip the state code prefix and "Local Government Area" when displaying, and keep the raw names in the data files.
- **Test:** no "kw" and no doubled "State" anywhere in chat, maps or downloads.
- **Size:** S.

---

## Phase 4: Maps, downloads and the interface
### 4.1 Map framing and wording (P11)
- The composite map opens zoomed to the state, not West Africa.
- The in-chat burden map is not cropped.
- The ITN map replaces "Tier 1 / Tier 2 (Urban Surplus)" with "First priority / Second priority (urban wards)", explains the urban threshold box in one line, and removes "Risk ranks: None".
- **Test:** look at each map at 1366×768.
- **Size:** M.

### 4.2 Downloaded dashboard and README (P12)
- Make the stat labels visible.
- Add % to coverage.
- Remove the non-existent "Very High Risk" category and the "maps will be displayed here" placeholder.
- **Test:** open the dashboard and README from the ZIP.
- **Size:** S.

### 4.3 Phone layout (P14)
- The header buttons (dark mode, Survey, Pre/Post Test) are off-screen at 390 px wide. Move them into a menu.
- The side rail collapses fully on phones.
- Hide the "Enter to send" hint on touch screens.
- **Test:** screenshots at 390×844.
- **Size:** M.

### 4.4 Other interface elements (P15)
- **Sidebar:** labels on rail icons (with 2.4).
- **Survey:** hide "Cognitive Assessment 0 / 0" and the empty "Survey Context:" label when no survey is active.
- **Pre/Post Test:** a research consent page that sits in the main toolbar. Move it out of the main toolbar; see Decision 4.
- **Privacy notice:** promises a Settings "review" option and a "Clear Session" button that don't exist. Either add them or correct the text.
- **Settings:** hide the raw session ID.
- **New Chat:** the warning says "This will clear your current chat", but the chat is kept in history. Change it to "Your current chat stays in your history".
- **Upload window:** open on the DHIS2 / facility data tab (the common case) instead of "I Have Complete Data".
- **Test:** click through each element.
- **Size:** M.

### 4.5 Production front door (P13)
- Spell out ChatMRPT, TPR and ITN on the landing page.
- Add a "Forgot password?" flow.
- Make Terms and Privacy links.
- Guest or demo access: see Decision 3.
- **Size:** M for the text and links; L for password reset if email sending isn't set up.

---

## Decisions (24 Nov 2026)
1. **Burden headline:** keep the current definition, cases over all years ÷ one year's population. Item 1.4 is reduced to:
   - wards without a DHIS2 match become "no data" (grey, not 0), which makes the headline and map averages agree;
   - better ward matching;
   - a flag on impossible values.
2. **Arena:** use the policy table as proposed, including a single answer for a new user's first message.
3. **Production login:** keep sign-up required. Item 4.5 is reduced to text, links and password reset.
4. **Survey and Pre/Post Test:** the study is still running, so move both into a "Research" menu.

## Order and effort
| Phase | Items | Effort |
|---|---|---|
| 1. Wrong or invented results | 1.1–1.5 | 3–4 days |
| 2. First contact and help | 2.1–2.5 | 3 days |
| 3. Following the workflow | 3.1–3.5 | 4–5 days |
| 4. Maps, downloads, interface | 4.1–4.5 | 3–4 days |

**If the demo date is close:** do 1.1, 1.2, 1.3, 2.3, 2.4 and 2.5 first, about 3 days in total. They cover everything a demo audience would see go wrong. Item 1.4 depends on Decision 1.
