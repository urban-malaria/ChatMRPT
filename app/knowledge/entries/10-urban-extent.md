---
id: urban-extent
title: Urban extent
aliases: [urban, urban extent, urban percentage, urban threshold, urbanicity, urban vs rural, rural, urban map]
summary: >
  Urban percentage is the share of a ward classified as urban. ChatMRPT maps it, uses it
  as an inverse risk variable, and uses an urban threshold to separate rural from urban
  wards (default 50% on the urban extent map, 75% in ITN planning).
status: draft
sources: []
open_questions:
  - Name the urban extent data source.
---
- The urban extent map greys out wards below a chosen threshold (default 50%; any value from 0 to 100 can be requested).
- In ITN planning, wards below the threshold are treated as rural and served first.
- In settlement classification, the threshold can filter which wards are offered for selection.
