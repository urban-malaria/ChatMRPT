---
id: risk-variables
title: Risk variables
aliases: [variables, risk variables, environmental variables, covariates, which variables, variable selection, geopolitical zone, zone, rainfall, ndwi, ndmi, evi, elevation, housing quality, soil wetness, nighttime lights, distance to water]
summary: >
  ChatMRPT selects risk variables automatically for the state's geopolitical zone, from
  environmental and settlement layers such as rainfall, NDWI/NDMI (water and moisture
  indices), soil wetness, elevation, distance to water bodies, housing quality,
  night-time lights and urban percentage. Users can also choose variables themselves.
status: draft
sources: []
open_questions:
  - Add the source dataset and year for each variable.
  - Confirm the zone variable lists are still current.
---
Default variables by geopolitical zone (urban percentage is always included because ITN planning needs it):
- **North Central:** night-time lights, housing quality, soil wetness, distance to water bodies, NDMI.
- **North East:** distance to water bodies, rainfall, soil wetness.
- **North West:** rainfall, NDWI, housing quality, elevation.
- **South East, South South, South West:** NDWI, housing quality, elevation.

The zone is detected from the state in the data. Missing values for a ward are filled with the average of neighbouring wards, or the overall average if no neighbour has a value. Users can ask for different variables, e.g. "run risk analysis with rainfall, elevation and housing quality".
