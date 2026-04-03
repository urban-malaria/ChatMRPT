---
name: malaria-analysis
description: Domain knowledge for malaria risk analysis and intervention targeting
---

# Malaria Analysis Domain Knowledge

## Primary Use Case
Epidemiological risk assessment for malaria intervention targeting in Nigerian states.

## Data Types
- **Demographic**: Ward-level population, household counts
- **Environmental**: Rainfall, temperature, vegetation indices, water bodies
- **Health Indicators**: Test positivity rates (TPR), incidence rates
- **Geospatial**: Shapefiles with ward boundaries, settlement classifications

## Analysis Methods

### Composite Scoring
- Normalize indicators to 0-1 scale
- Weight and combine into composite vulnerability score
- Rank wards by risk level

### PCA (Principal Component Analysis)
- Dimensionality reduction for multiple indicators
- Identify key contributing factors
- Create composite indices

### ITN Distribution Planning
- Population-based allocation
- Priority scoring by vulnerability
- Logistics optimization

## Key Metrics
- **TPR**: Test Positivity Rate - percentage of positive malaria tests
- **Vulnerability Score**: Composite risk index (0-1)
- **Priority Rank**: Ward ranking for intervention targeting

## Geographic Context
- Nigerian states (primary: Kano, also Adamawa, Kwara, Osun)
- Ward-level analysis (sub-LGA administrative units)
- LGA (Local Government Area) aggregations

## Output Types
- Interactive choropleth maps (Folium/Leaflet)
- Risk ranking tables (CSV)
- Statistical summaries
- Intervention recommendations

## Important Conventions
- Never hardcode state names or geographic identifiers
- Always detect state/region from uploaded data
- Support multiple Nigerian states dynamically
- Ward names may have spelling variations - use fuzzy matching
