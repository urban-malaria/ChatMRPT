---
id: pca
title: Principal component analysis (PCA)
aliases: [pca, principal component, principal components, principal component analysis, kmo, bartlett, pca skipped, pca not suitable, pca map]
summary: >
  PCA is the second ranking method. It standardises the variables, combines them into
  principal components, and scores wards using the first two or three components weighted
  by the variance they explain. It only runs if the data pass the KMO (>= 0.5) and
  Bartlett (p < 0.05) suitability tests; otherwise only composite scoring is used.
status: draft
sources: []
open_questions: []
---
1. Variables are standardised (mean 0, standard deviation 1). Variables with more than half their values missing are dropped.
   Variables where a higher value means lower risk (for example distance to water, housing quality, urban percentage) are reversed, so every variable points towards higher risk, as in composite scoring.
2. **Suitability tests.** Kaiser-Meyer-Olkin (KMO) must be at least 0.5 and Bartlett's test of sphericity must have p < 0.05. If either fails, PCA is skipped and ChatMRPT says why.
3. Components are extracted, and each ward's PCA score is the sum of its first two or three component scores weighted by each component's share of explained variance. A component's sign is arbitrary, so each is oriented so that its loadings sum to a positive number: a higher PCA score always means higher risk.
4. Wards are ranked by PCA score and split into High, Medium and Low Risk, the same way as composite scoring.

Comparing the composite and PCA rankings shows which wards are high risk under both methods.
