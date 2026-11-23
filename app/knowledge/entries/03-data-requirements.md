---
id: data-requirements
title: Data requirements
aliases: [what data, which data, data do i need, upload, uploading, file format, csv, excel, shapefile, columns, required columns, dhis2, data format, sample data]
summary: >
  For the guided workflow, upload health facility testing data (CSV, Excel or JSON),
  typically a DHIS2 export with facility, ward, LGA, period, and the number tested and
  positive by RDT and/or microscopy for each age group. For direct risk analysis, upload
  ward-level data (CSV/Excel) plus a zipped ward boundary shapefile.
status: draft
sources: []
open_questions:
  - Is there a minimum number of wards or months for reliable results?
---
**Facility testing data ("I Have TPR Data Only")**
- One row per facility per reporting period.
- Columns identifying the facility, its ward, LGA and state, and the reporting period.
- Counts of people tested and testing positive, by test method (RDT, microscopy) and age group (under 5, over 5, pregnant women).
- A facility level column (primary, secondary, tertiary) if results by facility level are wanted.
- Column names do not need to match exactly; ChatMRPT detects them and asks if unsure.

**Ward-level data ("I Have Complete Data")**
- A CSV or Excel file with one row per ward and a ward name column.
- A shapefile of the same wards, as a single ZIP file.

Sample datasets are available from the sidebar for trying the tool without your own data.
