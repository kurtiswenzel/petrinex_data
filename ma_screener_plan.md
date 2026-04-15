# M&A Acquisition Target Screener — 1-Hour Hackathon Plan

**Constraint:** 60 minutes to demo. Ship as a **Databricks Asset Bundle** extending the existing [databricks.yml](databricks.yml).

**The demo:** Open an AI/BI dashboard, filter to "Montney condensate wells under small operators," click the top result, show the Genie chat answering questions about the shortlist. Everything deployable with `databricks bundle deploy`.

---

## What We're Building (scope-controlled)

| # | Deliverable | Where it lives | Who builds it |
|---|---|---|---|
| 1 | Gold table: `shm.petrinex.gold_deal_screener` with inline `deal_score` | [setup_gold.py](setup_gold.py) notebook | DAB job |
| 2 | DAB job resource | [resources/gold_deal_screener_job.yml](resources/gold_deal_screener_job.yml) | Bundle |
| 3 | AI/BI Dashboard with map + ranked table + KPIs | [dashboards/ma_screener.lvdash.json](dashboards/ma_screener.lvdash.json) | Bundle |
| 4 | Dashboard DAB resource | [resources/ma_screener_dashboard.yml](resources/ma_screener_dashboard.yml) | Bundle |
| 5 | Genie Space for NL questions | Live (not in bundle yet — workspace object) | MCP at demo time |
| 6 | Updated [databricks.yml](databricks.yml) includes | Bundle | Bundle |

## Explicitly cut for time

- Databricks Apps / React front-end (too much build time)
- Lakeflow Declarative Pipeline (one-shot CTAS is enough)
- MLflow decline curve models (use early-vs-recent ratio instead)
- UC Python Functions for MRF royalty (inline SQL netback proxy is enough)
- Lakebase / sub-second lookup (AI/BI dashboard is fast enough)
- DBSCAN asset packages
- Custom PDF deal memos (judges can see the data live)
- Price-slider simulator

## Data reality — what we can actually compute

The demo is built around **wells that appear in `ngl_volumes`** — gas and condensate-rich wells with 24 months of monthly history (2024-01 → 2025-12). That's ~118K wells covering all Montney/Duvernay/Cardozo-adjacent plays — the richest slice of the dataset, and also the plays private equity actually wants to buy.

**Gold table columns:**
- Identity: `well_id`, `facility_id`, `operator_name`, `field_display_name`, `pool_name`, `formation`
- Geo: `latitude`, `longitude`, `region`
- Scale: `cum_boe`, `recent_boe_per_day`, `active_months`
- Fluid profile: `liquids_cut`, `water_cut`
- Trend: `production_trend_pct` (recent 6mo avg vs first 6mo avg)
- Economics: `netback_cad_per_boe` (proxy using WCS 75 CAD + AECO 2.5 CAD/GJ, 15% royalty, 12 CAD/boe opex)
- Environment: `emissions_intensity` (from facility_emissions)
- Seller motivation: `operator_well_count`, `operator_size_bucket` (Micro/Small/Mid/Large)
- **`deal_score` (0–100):** weighted composite computed inline with `PERCENT_RANK()` windows:
  - 35% liquids cut
  - 25% seller motivation (small + declining)
  - 20% production scale
  - 20% netback

## 60-Minute Runbook

| Min | Step | Tool |
|---|---|---|
| 0–5 | Write [setup_gold.py](setup_gold.py) notebook + [resources/gold_deal_screener_job.yml](resources/gold_deal_screener_job.yml) | Write |
| 5–15 | Execute the gold CTAS live against the workspace | `execute_sql` |
| 15–20 | Validate: `SELECT * FROM gold_deal_screener ORDER BY deal_score DESC LIMIT 20` — sanity check | `execute_sql` |
| 20–25 | Create Genie Space with curated example questions | `manage_genie` |
| 25–50 | Build [dashboards/ma_screener.lvdash.json](dashboards/ma_screener.lvdash.json) + deploy live + save DAB resource yaml | `manage_dashboard` |
| 50–55 | Update [databricks.yml](databricks.yml) includes + verify bundle still parses | Read/Edit |
| 55–60 | Rehearse 90-second demo on the deployed dashboard | Browser |

## 90-Second Demo Script

1. **(10s)** Open the dashboard. "Every gas/condensate well in Alberta — 118K wells, real 2024-2025 production."
2. **(10s)** Filter: Formation = Montney, Operator size = Micro (<30 wells). Map shrinks to ~150 pins.
3. **(15s)** Point at the ranked table. "Deal score combines liquids cut, seller motivation, production scale, and netback into one explainable number."
4. **(15s)** Click the top row. "This operator has 14 wells, production down 22% since early 2024, 73% liquids, netback 38 CAD/boe — classic distressed condensate asset."
5. **(20s)** Switch to Genie tab. Type *"Which operators have the highest average deal score in the Peace Country region?"* Watch Genie write SQL and return results.
6. **(15s)** Back to dashboard. Filter by different formation. Map re-renders. "Same app — any play, any operator size, any region."
7. **(5s)** Close: "Everything you just saw is a Databricks Asset Bundle. `bundle deploy` and it's live for your team."

## Databricks Surfaces on Display

Even in a 1-hour build, the demo hits **6 Databricks surfaces** — more than enough for a platform showcase:
1. **Unity Catalog** (gold table, lineage)
2. **Databricks SQL** (the CTAS and windows)
3. **Databricks Jobs via DAB** (the `gold_deal_screener_job`)
4. **AI/BI Dashboards** (the shortlist UI)
5. **Genie Spaces** (the NL interface)
6. **Databricks Asset Bundles** (the whole thing ships as code)

## Risk Mitigations

| Risk | Plan |
|---|---|
| Dashboard build is slow / flaky | Start simple: 1 map + 1 ranked table + 3 KPI tiles + filter. Iterate if time allows. |
| Genie accuracy poor | Seed 8 curated example questions; use those as talking points. |
| Gold CTAS errors on first run | Build incrementally via CTEs; test each CTE alone first. |
| Bundle deploy breaks existing `setup_data` job | Only add new files, don't edit existing ones. |

## Files shipped

```
petrinex_data-1/
├── databricks.yml                    # UPDATED: profile -> databricks-free-kw
├── setup_data.py                     # untouched
├── setup_gold.py                     # NEW — builds gold_deal_screener
├── ma_screener_plan.md               # this file
├── app/                              # NEW — Streamlit app with pydeck map
│   ├── app.py                        # Filters, KPIs, map, ranked table
│   ├── app.yaml                      # Streamlit command + valueFrom binding
│   └── requirements.txt              # pydeck, databricks-sql-connector, databricks-sdk
├── resources/
│   ├── setup_data_job.yml            # untouched
│   ├── gold_deal_screener_job.yml    # NEW — job to run setup_gold
│   ├── ma_screener_dashboard.yml     # NEW — dashboard DAB resource
│   └── ma_screener_app.yml           # NEW — app DAB resource
└── dashboards/
    └── ma_screener.lvdash.json       # NEW — dashboard definition
```

## Deployed artifacts in the workspace

| Artifact | ID / Name | URL |
|---|---|---|
| Gold table | `shm.petrinex.gold_deal_screener` | 106,130 ranked wells |
| AI/BI Dashboard | `01f1390c1f7c1322ab547a2f9ae2fd12` | [Open](https://dbc-f9439112-5585.cloud.databricks.com/sql/dashboardsv3/01f1390c1f7c1322ab547a2f9ae2fd12) |
| Genie Space | `01f1390bc24d1c8486fae7013fc1a25d` | "Petrinex M&A Deal Screener" |
| Streamlit App | `petrinex-ma-screener` | [Open](https://petrinex-ma-screener-3637137042377810.aws.databricksapps.com) |

## Debugging notes (issues hit, lessons for next time)

1. **CloudFetch is incompatible with Databricks Apps runtime.** The default behavior of databricks-sql-connector uploads result sets to `*.storage.cloud.databricks.com` and has the client download them. Apps compute can't reach that endpoint (`Connection refused`). Fix: pass `use_cloud_fetch=False` to `sql.connect`.
2. **`databricks.yml` profile was `DEFAULT` but the user's actual profile is `databricks-free-kw`.** Bundle commands failed silently until you added `--profile`. Fixed in bundle config.
3. **Free edition workspace has a 1-app limit.** Had to delete the previous app before creating the screener. If you rebuild, remember to save the previous app's source first.
4. **App service principal needs explicit UC grants.** Even though the warehouse is attached as a resource, the SP needs `USE CATALOG` + `USE SCHEMA` + `SELECT` on the gold table. Applied via SQL.
