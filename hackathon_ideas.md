# Petrinex Hackathon — Data App Ideas

Target: Databricks hackathon. Data is in [shm.petrinex](shm.petrinex) — 8 tables covering Alberta oil & gas production (~13M volumetric rows, 2024–2025), well-level NGL composition, facility geolocation, emissions (flare/vent/fuel), operator directory, and monthly benchmark prices (WTI/WCS/AECO). Royalty math reference already in repo at [mrf_royalty_reference.md](mrf_royalty_reference.md).

Each idea is scored for **wow factor**, **buildability in a hackathon window**, and the **Databricks surfaces** it shows off (judges love seeing the platform used).

---

## 1. Alberta Royalty & Economics Copilot ⭐ top pick
**Pitch:** A Databricks App where an analyst picks any facility/operator/month and instantly sees gross revenue, MRF royalty owed, netback, and sensitivity to price shocks — with a chat sidebar that answers "what if WTI drops 15%?"

**Why it wins:** The repo already ships the MRF formulas ([mrf_royalty_reference.md](mrf_royalty_reference.md)). No other team will have this. Ties directly to real money — judges get it instantly.

**Databricks surfaces:**
- **Databricks Apps** (React + FastAPI via apx, or Streamlit) for the UI
- **Unity Catalog Functions** to register MRF royalty formulas as callable SQL UDFs
- **Genie Space** embedded as the chat sidebar (NL → SQL over `volumetrics`, `market_prices`, `ngl_volumes`)
- **ai_forecast** to project next 6 months of price-adjusted royalty revenue
- **Model Serving** endpoint for a price-shock simulator

**Build order (aggressive):**
1. Port MRF Python from [mrf_royalty_reference.md](mrf_royalty_reference.md) → UC Python UDF
2. One Lakeflow view joining `volumetrics` + `market_prices` + emissions
3. Genie space pointed at that view
4. apx app with a map (facility picker) + KPI cards + scenario sliders

---

## 2. Flaring & Emissions Intensity ESG Tracker
**Pitch:** A map-first dashboard that ranks every Alberta operator by flare intensity (m³ flared per m³ produced), vent intensity, and month-over-month trend. Click an operator → drill to the worst facilities → see a Genie chat to investigate.

**Why it wins:** ESG is a hot-button topic and this hits real regulatory pain. `facility_emissions` + `facilities` (with lat/lon already converted from DLS!) gives you an instant map story.

**Databricks surfaces:**
- **AI/BI Dashboard** with a geo map (facilities are pre-geolocated in WGS84)
- **Lakeflow Declarative Pipeline** for a bronze → silver → gold medallion: raw emissions → intensity metrics → operator scorecards
- **Unity Catalog Metric Views** for governed ESG KPIs (flare_intensity, vent_intensity, emission_trend)
- **ai_classify** to auto-tag facilities as "improving / stable / worsening"
- **Lakebase synced table** to power the map app with sub-second lookups

**Killer demo moment:** Toggle between a baseline month and the worst month — watch the map light up red, then click the #1 offender and have the Genie chat explain what happened.

---

## 3. Type-Curve Forecaster & Decline Curve Analytics
**Pitch:** Pick a formation (Montney, Cardium, Mannville…), click "generate type curve," and get a forecast of a new well's production for the next 24 months plus a confidence band. Compare against actual wells in that formation.

**Why it wins:** Decline curve analysis (DCA) is THE core workflow in upstream oil & gas. Having a one-click version backed by real Alberta data is genuinely useful, not just a demo.

**Databricks surfaces:**
- **ai_forecast** (built-in SQL AI function) for zero-code baseline forecasts per formation
- **MLflow** for a custom Arps decline model (q_i, D_i, b) registered in UC
- **Model Serving** endpoint to score new well inputs on demand
- **Vector Search** on well "fingerprints" (formation + early production profile) to find the 10 most similar historical wells to any candidate — a "comparable wells" recommender
- **MLflow GenAI evaluation** to score the forecast quality

**Killer demo moment:** The Vector Search "find similar wells" piece — drop a new well and watch 10 analogs appear on a map with their actual decline curves overlaid.

---

## 4. M&A / Acquisition Target Screener
**Pitch:** "I want to buy wells in the Montney with GOR < X, declining but not dead, held by operators with < 50 wells, at current AECO I can pay back in 3 years." Hit run → get a ranked list with maps, economics, and a one-page PDF deal memo.

**Why it wins:** Feels like a Bloomberg terminal moment. Judges see a real business workflow compressed to 30 seconds.

**Databricks surfaces:**
- **Genie Space** as the query engine (NL screening criteria)
- **UC Metric Views** for standardized "payback," "netback," "GOR," "water cut" definitions
- **ai_query** to generate the narrative in the deal memo
- **databricks-unstructured-pdf-generation** skill → one-click PDF export to a UC volume
- **Lakebase** backing an apx app for the shortlist UI

---

## 5. NGL Yield & Processing Optimizer
**Pitch:** The `ngl_volumes` table has Mix vs Spec volumes for ethane/propane/butane/pentane — meaning you can see fractionation shrinkage. Build an app that ranks gas plants by NGL recovery efficiency and flags under-performers.

**Why it wins:** Niche but differentiated — nobody else at a hackathon will know what NGL shrinkage is, which makes it memorable. Real midstream value.

**Databricks surfaces:**
- **Structured Streaming** simulated real-time ingest from the parquet volume (looks impressive in a demo)
- **Unity Catalog Monitors** on `ngl_volumes` to auto-alert on yield drift
- **AI/BI Dashboard** with waterfall charts of component-by-component yield
- **ai_query** in a "root cause" narrative column

---

## 6. Regulatory Compliance Copilot (RAG)
**Pitch:** Ingest Alberta MRF regulations, AER directives, and the royalty reference doc. A chat app answers questions like "for a Montney condensate-rich well drilled in 2024, what royalty rate applies at current WCS?" — grounded in both the regulations AND the live production data.

**Why it wins:** This is the hot RAG-over-enterprise-data pattern. The repo already has [mrf_royalty_reference.md](mrf_royalty_reference.md) as the starter corpus.

**Databricks surfaces:**
- **Vector Search** (storage-optimized) over regulatory docs
- **ai_parse_document** for any PDF regulations you add
- **Knowledge Assistant (Agent Bricks KA)** — lowest-code path
- **Supervisor Agent (MAS)** combining KA (regulations) + Genie (live data) — a "dual brain" demo
- **MLflow tracing + evaluation** so judges see production-grade observability

**Killer demo moment:** Ask a question that requires both regulation knowledge AND a live SQL query → watch the supervisor route to both agents and synthesize.

---

## 7. Operator Benchmark & Peer Comparison
**Pitch:** A self-serve tool where any of Alberta's ~500 operators can see themselves ranked vs. anonymized peers on production growth, emissions intensity, NGL recovery, and royalty efficiency.

**Why it wins:** Low build cost, high visual impact, clearly demo-able.

**Databricks surfaces:**
- **AI/BI Dashboard** with parameterized operator filter
- **Metric Views** for the benchmark KPIs
- **Genie Space** for the "ask anything" sidebar
- **ai_analyze_sentiment** + `ai_gen` for an executive summary paragraph

---

## Recommendation

For a hackathon, **pick #1 (Royalty Copilot)** or **#2 (ESG Tracker)** as your primary:
- **#1** if judges are business/finance leaning — the royalty math is unique IP for this dataset and the repo hands you the formulas
- **#2** if judges are platform/data leaning — it touches the most Databricks surfaces (DLP pipeline, metric views, Lakebase, AI/BI, Genie, ai_classify) in one build

**Stretch:** combine #1 and #6 — the royalty app gets a RAG copilot as its chat sidebar. That's a Databricks Apps + Agent Bricks + Genie + ai_forecast + UC Functions demo in one, which is about as much surface area as you can show in a single app.

## Quick-win build blocks you already have
- Data is loaded ([setup_data.py](setup_data.py), [databricks.yml](databricks.yml))
- Geolocation done (lat/lon on `facilities`)
- MRF formulas written ([mrf_royalty_reference.md](mrf_royalty_reference.md))
- Price benchmarks loaded (`market_prices`)
- Coding conventions & SQL patterns documented ([petrinex_instructions.md](petrinex_instructions.md))

Biggest time sinks to avoid: don't hand-roll a forecasting model when `ai_forecast` exists, don't build a map from scratch when AI/BI Dashboards have one, don't build auth when Databricks Apps gives it to you free.
