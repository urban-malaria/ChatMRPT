---
id: workflow
title: ChatMRPT workflow
aliases: [workflow, steps, process, tpr workflow, start the tpr workflow, guided workflow, how do i start, where do i start, getting started, next step, what next, pipeline]
summary: >
  The usual sequence is: upload data, run the guided TPR workflow (choose facility level
  and age group) to calculate ward malaria burden, run risk analysis to rank wards, then
  explore maps, plan ITN distribution, classify settlements and export results.
status: draft
sources: []
open_questions:
  - The guided workflow is still called the "TPR workflow" although it now reports burden per 1,000 population. Keep the name?
---
1. **Upload.** In the upload window choose "I Have TPR Data Only" for health facility testing data (CSV, Excel or JSON), or "I Have Complete Data" for ward-level data (CSV/Excel) plus a ward boundary shapefile (ZIP).
2. **Guided TPR workflow** (facility testing data). Type "start the TPR workflow". ChatMRPT asks for the state (only if the file has several), the facility level (primary, secondary, tertiary or all) and the age group (under 5, over 5, pregnant women or all ages). It then calculates malaria burden per ward, maps it, and adds environmental variables for each ward.
3. **Risk analysis.** Ranks wards using composite scoring and PCA and classifies them as High, Medium or Low Risk. With several years of data, per-year rankings combine environmental risk and burden.
4. **Maps and questions.** Map any variable, view vulnerability and urban extent maps, and ask questions about the data. Each chart has an "Explain this visualization" button.
5. **ITN planning.** Allocate a given number of nets to wards in risk order (needs total nets and average household size).
6. **Settlement classification.** Label grid cells in selected wards as Formal, Informal, Slum, Rural or No Buildings/Avoid Area.
7. **Export.** Download maps, tables and reports from the export menu.

Steps must run in order: ITN planning and risk-based ward selection need risk analysis first. Facility level or age group can be changed later without re-uploading.
