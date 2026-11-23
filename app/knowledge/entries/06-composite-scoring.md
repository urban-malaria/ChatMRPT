---
id: composite-scoring
title: Composite scoring
aliases: [composite, composite score, composite scoring, composite method, composite analysis, composite score maps, model breakdown, how are wards ranked, ranking method, normalization, normalisation]
summary: >
  Composite scoring rescales each risk variable to 0-1, averages the variables for
  many combinations of them (models), and ranks wards by their median score across
  those models. Higher scores mean higher risk.
status: draft
sources: []
open_questions:
  - Inverse variables are transformed as 1/x before rescaling (not 1 - x). Confirm this is intended.
---
1. **Direction.** Each variable is marked direct (higher value = higher risk, e.g. rainfall, population density, vegetation) or inverse (higher value = lower risk, e.g. elevation, distance to water, housing quality, urban percentage).
2. **Rescaling.** Direct variables are min-max rescaled to 0-1. Inverse variables are inverted (1 / value) and then rescaled, so high still means high risk.
3. **Models.** Each model is the plain average of the rescaled values for one combination of variables (pairs, triples and so on). With up to five variables all combinations are used; with more, a limited set.
4. **Ranking.** Each ward's overall score is its median score across all models. Wards are ranked from highest to lowest, then split into High, Medium and Low Risk.

Using many models and the median makes the ranking less sensitive to any single variable. Composite score maps show each model's result side by side.
