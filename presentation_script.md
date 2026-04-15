# Presentation Script — Petrinex M&A Deal Screener

**Audience:** Databricks hackathon judges
**Length target:** 5-6 minutes (plus Q&A)
**Tone:** Confident, specific, numbers-forward. Don't over-explain the stack — show it.

---

## 0. Setup before you go on stage

- [ ] Browser tab 1: **App** — [petrinex-ma-screener](https://petrinex-ma-screener-3637137042377810.aws.databricksapps.com) — already loaded with default view
- [ ] Browser tab 2: **AI/BI Dashboard** — [deal screener dashboard](https://dbc-f9439112-5585.cloud.databricks.com/sql/dashboardsv3/01f1390c1f7c1322ab547a2f9ae2fd12)
- [ ] Browser tab 3: **Genie Space** — [Petrinex M&A Deal Screener](https://dbc-f9439112-5585.cloud.databricks.com/genie/rooms/01f1390bc24d1c8486fae7013fc1a25d)
- [ ] Browser tab 4: **VS Code** with [ma_screener_plan.md](ma_screener_plan.md) or [setup_gold.py](setup_gold.py) — for when you say "here's the whole thing as code"
- [ ] Terminal open with `databricks bundle validate` ready to run (optional flex move)

---

## 1. Opening — 30 seconds

> "I'm going to show you an app that replaces a spreadsheet Alberta BD teams spend weeks building every quarter — a ranked acquisition screener for oil and gas wells, backed by real Petrinex production data, shipped end-to-end as a Databricks Asset Bundle."

**Beat:** one sentence. Don't elaborate yet. Move to the problem.

---

## 2. The problem — 45 seconds

> "When a private equity energy fund or a junior E&P wants to acquire wells in Alberta, they ask: which assets are liquids-rich, who's a motivated seller, what's the economics at today's strip? Today that answer lives in a 50-tab Excel model built by an analyst over two weeks. Every time you change WTI or want a different formation you rebuild it."
>
> "I built the same tool in one afternoon. 106,000 wells, 255 operators, ranked by a composite deal score you can filter live."

**Beat:** pause, then switch to the app tab.

---

## 3. The data — 30 seconds

> "The data is Petrinex — Alberta's official oil and gas reporting system. Every well, every facility, every monthly production volume for 2024 and 2025. It's already loaded into Unity Catalog as a schema called `shm.petrinex` by the existing bundle."
>
> "The relevant slice is 118,000 gas and condensate wells — that's the richest data in the set and also what M&A teams actually care about. All ranked here."

**What to show:** just the app title and KPI row — "Wells Screened: 106K, Operators: 255, Avg Deal Score: 25.8".

---

## 4. Architecture — 90 seconds

> "The architecture is four layers. Let me show you on the screen."
>
> 1. **Silver tables** — Petrinex raw tables in Unity Catalog, loaded by the existing `setup_data` job.
> 2. **Gold table** — `shm.petrinex.gold_deal_screener`. One row per well, with 30-odd columns including the composite `deal_score`. Built by a single SQL CTAS with four CTEs. Takes about a minute to build.
> 3. **Three surfaces on top** — this Streamlit app with a pydeck map, a Databricks AI/BI dashboard for operators who want charts, and a Genie Space for anyone who wants to ask questions in English.
> 4. **Everything is one Databricks Asset Bundle.** `databricks bundle deploy` rebuilds the whole thing in a fresh workspace — the job, the table, the dashboard, and this app. Version-controlled, CI-ready."

**Optional flex:** switch to your terminal and run `databricks bundle validate` — takes two seconds and shows "Validation OK!".

**Surfaces touched:** Unity Catalog, Databricks SQL, Databricks Jobs, AI/BI Dashboards, Genie Spaces, Databricks Apps, Databricks Asset Bundles. **Seven.**

---

## 5. The deal score — how the metric is designed — 75 seconds

> "Every deal scoring model is a weighted sum of things you care about. I picked four, and I can explain all of them in one sentence each."
>
> 1. **Liquids cut — 35%.** What share of the production is condensate plus oil versus dry gas. Liquids fetch a higher netback, so condensate-rich wells always out-rank dry gas. This is the single biggest signal for acquisition economics in Alberta right now.
>
> 2. **Seller motivation — 25%.** Is the operator likely to actually sell? Proxy: small operator (under 30 wells) plus declining production. If production is down more than 10% year-over-year and they have fewer than 30 wells, you get the full 25 points. If they're mid-sized and stable, you get almost nothing.
>
> 3. **Production scale — 20%.** How much are we actually buying? Percentile rank of recent 6-month average BOE per day. Tiny wells get zero, flowing 100 boe/day gets full credit.
>
> 4. **Netback — 20%.** Dollars per BOE after royalty and opex. Proxy formula: WCS at 75 CAD, AECO at 2.5 CAD per GJ, 15% royalty, 12 CAD per BOE opex — all industry-standard rules of thumb. Percentile rank so the score is always comparable across plays.
>
> "The whole thing is computed inline with SQL `PERCENT_RANK()` window functions, no Python model, no training data. Explainable in 30 seconds to your CFO."

**Beat:** this is your differentiator. Don't rush this section. The deal score IS the IP.

---

## 6. Live demo — 2 minutes

> "OK — live demo. Start with the universe: 106,000 wells, everything."

**[Click]** Show the map. Pause for 2 seconds on the full view — thousands of dots across Alberta.

> "Now let's pretend I'm a PE fund that wants liquids-rich Montney assets from distressed operators."

**[Click]** Sidebar → Formation = **Montney**, Operator size = **Micro (<30)**, Min deal score = **80**.

> "The map re-renders. The KPIs update. Wells in shortlist — 40-ish, total BOE/day around 2,000, average deal score in the high 80s. Every dot is a real well I could make an offer on tomorrow."

**[Click]** Scroll to the ranked table.

> "Number one is Rally Canada Resources — 98% liquids cut, production down 17%, netback 50 Canadian per BOE. Small operator, 14 wells total. That's a classic distressed condensate asset."
>
> "Look at row 7 — the operator is literally named **Acquisition Oil Corp**. Can't make it up."

**[Click]** Open the CSV download button. Don't actually download — just show it exists.

> "One click and your investment committee has the shortlist."

**[Click]** Open the **AI/BI Dashboard** link in the sidebar.

> "Same data, chart view, for the folks who want operator comparisons. Here's the top 15 distressed operators by average deal score. Here's the best formations — Ellerslie, Nordegg, Elkton, Duvernay."

**[Click]** Open the **Genie** link.

> "And for anyone who doesn't want to learn my UI — natural language."

**[Type into Genie]** *"Which Micro operators have the highest average deal score in the Duvernay formation?"*

> "Genie writes the SQL, runs it on the same gold table, returns results. Same data, same deal score, three different front doors."

---

## 7. What makes this different — 30 seconds

> "Three things I want you to remember:"
>
> 1. **Real workflow.** This replaces an actual spreadsheet a real team uses. Not a toy demo.
> 2. **Explainable metric.** The deal score is 4 numbers in a weighted sum. You can debate the weights but you can't hide behind a black-box model.
> 3. **Ships as code.** Seven Databricks surfaces in one bundle. `databricks bundle deploy` and any team can stand this up in their own workspace in 60 seconds."

---

## 8. Close — 15 seconds

> "The demo is live at `petrinex-ma-screener.databricksapps.com`, the repo is on my laptop, and the question I usually get is: *what about the royalty calculation*? The answer is the Alberta MRF reference markdown in the repo already has the formulas — wiring them into the netback column is the next PR."
>
> "Thank you. Happy to take questions."

---

## Q&A preparation — answers to likely questions

**"How did you pick the 35/25/20/20 weights?"**
> Honest answer: I started with a prior — liquids cut is the biggest signal in Alberta right now, and seller motivation is second because a great asset you can't buy isn't a deal. The weights are explicit and tunable in the SQL. Next iteration would be user-tunable sliders on the app, which is maybe 20 lines of code.

**"What's missing for this to be a real product?"**
> Three things: land title and mineral rights lookup (not in Petrinex — needs IHS or GeoLogic feed), real acquisition cost data (we use a 50k-per-flowing-boe-per-day proxy, which is industry standard), and decline curve fitting (I use a ratio of last 6 months to first 6 months, which is a rough proxy; Arps fits would be more accurate).

**"Why wasn't the Alberta royalty formula used directly?"**
> The MRF formulas are in the repo in `mrf_royalty_reference.md`. I used a simpler netback proxy for time reasons. The next iteration registers the MRF math as a Unity Catalog Python function so the deal score can call it per-well.

**"Why Streamlit and not Dash or React?"**
> Time to pretty UI per line of code. For a hackathon, Streamlit plus pydeck is the fastest path to an interactive map. A production version would probably be React plus FastAPI for tighter control over the detail drawer.

**"How much did this cost to build?"**
> Build time, about 90 minutes — most of which was the first pass of the gold table SQL and one bug where `databricks-sql-connector` couldn't use CloudFetch from the Apps runtime. Compute cost, basically zero — a Serverless Starter warehouse and a Databricks App on Free Edition.

**"How would you scale this?"**
> The gold table is 106K rows, so it fits in memory client-side for the app. For 10 million wells I'd move the filtering to SQL (WHERE clauses passed through from the sidebar) and paginate the table. The architecture doesn't change — just the boundary between SQL and Pandas moves left.

---

## Architecture diagram (for your slides if you want one)

```
┌─────────────────────────────────────────────────────────────┐
│                  DATABRICKS ASSET BUNDLE                    │
│                                                             │
│  ┌────────────┐      ┌────────────────┐     ┌────────────┐  │
│  │ setup_data │ ───> │ Silver tables  │     │ databricks.│  │
│  │    job     │      │ shm.petrinex.* │     │    yml     │  │
│  └────────────┘      └────────┬───────┘     └────────────┘  │
│                               │                             │
│                               v                             │
│                    ┌────────────────────┐                   │
│                    │ setup_gold job     │                   │
│                    │ (SQL CTAS + CTEs)  │                   │
│                    └────────┬───────────┘                   │
│                             │                               │
│                             v                               │
│                    ┌────────────────────┐                   │
│                    │ gold_deal_screener │                   │
│                    │ 106K ranked wells  │                   │
│                    └────────┬───────────┘                   │
│                             │                               │
│       ┌─────────────────────┼─────────────────────┐         │
│       │                     │                     │         │
│       v                     v                     v         │
│  ┌─────────┐         ┌─────────────┐      ┌────────────┐    │
│  │ Genie   │         │ AI/BI       │      │ Streamlit  │    │
│  │ Space   │         │ Dashboard   │      │ App        │    │
│  │ (NL->SQL│         │ (map+chart) │      │ (pydeck)   │    │
│  └─────────┘         └─────────────┘      └────────────┘    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
           |                      |                    |
      Genie URL           Dashboard URL          App URL
```

## Deal score formula reference card

```
deal_score = ROUND(
    35.0 * PERCENT_RANK() OVER (ORDER BY liquids_cut)
  + 25.0 * seller_motivation_score
  + 20.0 * PERCENT_RANK() OVER (ORDER BY recent_boe_per_day)
  + 20.0 * PERCENT_RANK() OVER (ORDER BY netback_cad_per_boe)
, 1)

seller_motivation_score =
  CASE
    WHEN operator_well_count < 30  AND production_trend_pct < -0.1 THEN 1.00
    WHEN operator_well_count < 30                                  THEN 0.65
    WHEN operator_well_count < 100 AND production_trend_pct < -0.1 THEN 0.55
    WHEN operator_well_count < 100                                 THEN 0.35
    WHEN production_trend_pct < -0.1                               THEN 0.20
    ELSE                                                                0.05
  END

netback_cad_per_boe =
    (liquids_m3 * 6.293 * 75 * 0.85)          -- WCS CAD/bbl after 15% royalty+adj
  + (gas_e3m3  *  87.5   * 0.85)              -- AECO CAD/e3m3 after 15%
  - (total_boe * 12)                          -- opex
  / total_boe
```
