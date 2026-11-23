---
id: malaria-burden
title: Malaria burden
aliases: [burden, malaria burden, burden per 1000, per 1,000, per 1000, cases per 1000, incidence, burden map]
summary: >
  Malaria burden is confirmed positive cases per 1,000 population in a ward:
  (positive cases / ward population) x 1,000. It is the main ward-level measure the
  guided workflow produces, and lets wards of different sizes be compared fairly.
status: draft
sources: []
open_questions:
  - Name the population source (the code uses national population rasters; confirm dataset and year, e.g. WorldPop).
---
- **Positive cases** are summed over all facilities in the ward and all reporting periods in the data (or within each year, for per-year results). When both RDT and microscopy data exist, ChatMRPT uses the method with the higher positivity rate for that ward, so the same patients are not counted twice.
- **Population** is estimated for each ward from population rasters, matched to the chosen age group: total population for all ages, under-5 population for children under 5, total minus under-5 for over 5, and women aged 15-49 for pregnant women.
- Values are capped at 1,000 per 1,000, since higher values are not possible.
- LGA averages are population-weighted: total positives divided by total population in the LGA.
- Burden reflects cases confirmed at health facilities, so it depends on testing and care-seeking as well as transmission.
