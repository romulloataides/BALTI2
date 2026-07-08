# Graph Report - BALTI2-work  (2026-07-08)

## Corpus Check
- 29 files · ~240,303 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 296 nodes · 531 edges · 22 communities (20 shown, 2 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 5 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `a677d686`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]

## God Nodes (most connected - your core abstractions)
1. `renderReport()` - 20 edges
2. `fetch()` - 18 edges
3. `BALTI2` - 13 edges
4. `setStatus()` - 11 edges
5. `Supabase setup (Phases 5, 6, and 7)` - 11 edges
6. `2. Source Profiles` - 10 edges
7. `loadLayer()` - 9 edges
8. `refreshFollows()` - 9 edges
9. `answerInterpretation()` - 9 edges
10. `Design System — Project Trace / Project Compass` - 9 edges

## Surprising Connections (you probably didn't know these)
- `arcgis()` --calls--> `fetch()`  [INFERRED]
  live311.js → worker/index.js
- `suggestAddress()` --calls--> `fetch()`  [INFERRED]
  live311.js → worker/index.js
- `geocodeAddress()` --calls--> `fetch()`  [INFERRED]
  live311.js → worker/index.js
- `loadDashboardData()` --calls--> `fetch()`  [INFERRED]
  supabase/functions/analysis-desk/index.ts → worker/index.js
- `openAiResponsesRequest()` --calls--> `fetch()`  [INFERRED]
  supabase/functions/analysis-desk/index.ts → worker/index.js

## Import Cycles
- None detected.

## Communities (22 total, 2 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.09
Nodes (45): DASHBOARD_DATA, allowedOrigins(), answerComparison(), answerDeterministically(), answerInterpretation(), answerTrend(), askModel(), averageMetric() (+37 more)

### Community 1 - "Community 1"
Cohesion: 0.10
Nodes (35): adminClient, AnalysisRequest, arrayOfStrings(), authClient, authenticateAdmin(), buildInstructions(), conversationInputFromMessages(), corsHeaders (+27 more)

### Community 2 - "Community 2"
Cohesion: 0.07
Nodes (26): 1. Executive Summary, 2.1 BNIA Vital Signs, 2.2 City Health Dashboard, 2.3 CDC PLACES, 2.4 Open Baltimore 311, 2.5 ACS 5-Year Estimates, 2.6 EPA EJScreen, 2.7 Baltimore Area Survey (+18 more)

### Community 3 - "Community 3"
Cohesion: 0.15
Nodes (17): addressQuery(), addressSuggestionItems(), arcgis(), copyPacket(), copySrLink(), copyText(), geocodeAddress(), packetText() (+9 more)

### Community 4 - "Community 4"
Cohesion: 0.14
Nodes (13): BALTI2, BNIA longitudinal input, Current architecture, Data schema, Deployment, GitHub Pages Setup Reference, Live prototype backend, Migration (+5 more)

### Community 5 - "Community 5"
Cohesion: 0.23
Nodes (13): addFollowRecord(), confirmCandidate(), follow(), handleDeepLink(), normalizeFeature(), openSr(), queryExact(), sql() (+5 more)

### Community 6 - "Community 6"
Cohesion: 0.21
Nodes (13): bboxKey(), cancelPin(), clearReportPin(), closeReport(), finishPin(), hidePinGuide(), loadLayer(), openReport() (+5 more)

### Community 7 - "Community 7"
Cohesion: 0.22
Nodes (12): BENCHMARK_METRICS, dataPath, deriveBenchmarkRecord(), INVERSE_METRICS, LEGACY_RATES, legacySeries(), mean(), migrated (+4 more)

### Community 8 - "Community 8"
Cohesion: 0.24
Nodes (12): candidateHtml(), chooseAddress(), findAddress(), generatePacket(), goToAddress(), now(), openPortal(), renderReport() (+4 more)

### Community 9 - "Community 9"
Cohesion: 0.17
Nodes (11): 10. Optional CI secrets (GitHub), 1. Create a Supabase project, 2. Run the schema migration, 3. Allowlist admin emails, 4. Get API credentials, 5. Configure local credentials, 6. Configure auth redirect URLs, 7. Add Edge Function secrets (+3 more)

### Community 10 - "Community 10"
Cohesion: 0.20
Nodes (9): Color strategy: Restrained, Component patterns, Data palette (choropleth + bars), Design System — Project Trace / Project Compass, Motion, Palette (OKLCH), Spacing rhythm, Typography (+1 more)

### Community 11 - "Community 11"
Cohesion: 0.24
Nodes (10): beginPin(), ensureModal(), ensurePinGuide(), handlePinContainerClick(), hideReportModal(), pinCoord(), reportPinIcon(), setReportLocation() (+2 more)

### Community 12 - "Community 12"
Cohesion: 0.27
Nodes (10): bindMap(), catOptions(), init(), loadFollows(), mergeFollows(), mountPanel(), refreshFollows(), renderFollows() (+2 more)

### Community 13 - "Community 13"
Cohesion: 0.20
Nodes (9): Anti-references, Brand, Core features (non-negotiable), Product Purpose, Project Trace / Project Compass — Baltimore Health Dashboard, Register, Strategic principles, Tone (+1 more)

### Community 14 - "Community 14"
Cohesion: 0.22
Nodes (8): Completed & verified (this session cycle), Current status / next work, Environment quirks, Ideas backlog (from NYU CHDB gap analysis, ranked), People / asks pending, Repo state, Session Handoff — Baltimore Health Dashboard (BALTI2), Suggested skills

### Community 15 - "Community 15"
Cohesion: 0.25
Nodes (7): Asthma Metric Upgrade TODO, Citywide Min/Max Bounds Used (Current Snapshot), Composite Formula, Health Index (`hi`) Methodology, Missing-Data Rule, Normalization, Status

### Community 16 - "Community 16"
Cohesion: 0.38
Nodes (7): bboxAround(), candidateMatches(), cat(), findCandidates(), inWhere(), layerParams(), whereForCat()

### Community 17 - "Community 17"
Cohesion: 0.38
Nodes (7): chip(), esc(), fmtDate(), js(), marker(), popupHtml(), statusBucket()

### Community 18 - "Community 18"
Cohesion: 0.29
Nodes (6): API Keys To Get, BALTI2 Wiring Rule, City Health Dashboard Source Notes, Current App Wiring, Metric 42, What CHDB Provides

## Knowledge Gaps
- **104 isolated node(s):** `YEARS`, `LEGACY_RATES`, `BENCHMARK_METRICS`, `INVERSE_METRICS`, `dataPath` (+99 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **2 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `fetch()` connect `Community 0` to `Community 1`, `Community 3`?**
  _High betweenness centrality (0.231) - this node is a cross-community bridge._
- **Why does `arcgis()` connect `Community 3` to `Community 0`, `Community 5`, `Community 6`, `Community 12`, `Community 16`?**
  _High betweenness centrality (0.073) - this node is a cross-community bridge._
- **Why does `loadDashboardData()` connect `Community 1` to `Community 0`?**
  _High betweenness centrality (0.069) - this node is a cross-community bridge._
- **Are the 5 inferred relationships involving `fetch()` (e.g. with `loadDashboardData()` and `openAiResponsesRequest()`) actually correct?**
  _`fetch()` has 5 INFERRED edges - model-reasoned connections that need verification._
- **What connects `YEARS`, `LEGACY_RATES`, `BENCHMARK_METRICS` to the rest of the system?**
  _104 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.08776595744680851 - nodes in this community are weakly interconnected._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.10256410256410256 - nodes in this community are weakly interconnected._