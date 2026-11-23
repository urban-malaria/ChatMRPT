---
id: tpr
title: Test positivity rate (TPR)
aliases: [tpr, test positivity rate, test positivity, positivity rate, positivity, u5_tpr_rdt, tpr trend, tpr trends]
summary: >
  TPR (Test Positivity Rate) is the percentage of people tested for malaria whose test
  is positive: positive tests / total tests x 100. ChatMRPT uses TPR for trends over time;
  the ward maps and risk analysis use malaria burden per 1,000 population.
status: draft
sources: []
open_questions:
  - Some risk-analysis variable lists still refer to u5_tpr_rdt. Confirm whether TPR or burden enters the composite score in each case.
---
- Formula: TPR = (number positive / number tested) x 100.
- It can be calculated by test method (RDT or microscopy), age group and facility level.
- A rising TPR over time suggests worsening transmission; a falling TPR suggests improvement, provided testing practices have not changed.
- TPR adjusts for how many tests were done, so it is more comparable than raw case counts, but it is affected by who gets tested.
- The guided workflow is named the "TPR workflow" for historical reasons; it now reports burden per 1,000 population for each ward.
