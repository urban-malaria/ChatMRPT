---
id: itn-planning
title: ITN (bed net) planning
aliases: [itn, itns, bed net, bed nets, nets, llin, net distribution, itn distribution, itn planning, allocate nets, net allocation, campaign]
summary: >
  ITN planning allocates a given number of insecticide-treated nets to wards in risk
  order. Each ward needs one net per 1.8 people (at least one per household). Rural
  wards are served first, then urban wards, until the nets run out. It needs the total
  number of nets and the average household size, and runs after risk analysis.
status: draft
sources: []
open_questions:
  - Cite the source for 1 net per 1.8 people.
---
1. Ward population comes from the built-in population dataset.
2. Nets needed per ward = the larger of population / 1.8 and the number of households (population / average household size).
3. Wards below the urban threshold (default 75% urban) are served first, highest risk first; urban wards get any remaining nets.
4. If nets run out, the last ward served gets a partial allocation.
5. Results include nets allocated per ward, population covered, and maps.

ChatMRPT asks for the total nets and average household size if they are not given; it does not assume them.
