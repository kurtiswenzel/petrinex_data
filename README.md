# Petrinex Data -- Alberta Oil & Gas

Real Alberta oil and gas production data sourced from [Petrinex Public Data](https://www.petrinex.ca/PD/Pages/default.aspx) (2024-2025).

---

## Quick Start

### Option A: Run from browser

1. Upload parquet to a UC Volume:
   ```bash
   databricks fs cp -r data/ dbfs:/Volumes/<catalog>/<schema>/dataset/parquet/ --overwrite
   ```
2. Import `setup_data.py` into your workspace
3. Set `catalog` and `schema` widgets, attach to serverless or any cluster
4. **Run All** (~2 minutes)

### Option B: Bundle deploy

```bash
# One-time: upload parquet to volume
databricks fs cp -r data/ dbfs:/Volumes/shm/petrinex/dataset/parquet/ --overwrite

# Deploy and run
databricks bundle deploy
databricks bundle run setup_data
```

Override catalog/schema via `databricks.yml` variables.

---

## Tables

| Table | Rows | Description |
|-------|------|-------------|
| **volumetrics** | ~13M | Facility-level monthly production volumes |
| **ngl_volumes** | ~2.5M | Well-level NGL production |
| **facilities** | ~25K | Facilities with lat/lon (converted from DLS) |
| **operators** | ~500 | Operator profiles from production data |
| **wells** | ~118K | Per-well summary with formation and field names |
| **field_codes** | 84 | AER field code-to-name mappings |
| **facility_emissions** | ~367K | Flaring, venting, and fuel gas per facility per month |
| **market_prices** | 14 | Monthly WTI, WCS, AECO (Jan 2025 -- Feb 2026) |

---

## Data Model

```
volumetrics (13M)          ngl_volumes (2.5M)
     |                          |
     +---> facilities (25K)     +---> wells (118K)
     |                                 |
     +---> operators (500)             +---> field_codes (84)
     |
     +---> facility_emissions (367K)

market_prices (14) -- standalone reference
```

### Key Relationships

- `volumetrics.ReportingFacilityID` = `facilities.facility_id`
- `volumetrics.OperatorBAID` = `operators.operator_baid`
- `ngl_volumes.WellID` = `wells.well_id`
- `wells.facility_id` = `facilities.facility_id`
- `wells.field_name` = `field_codes.field_code`
- `facility_emissions.facility_id` = `facilities.facility_id`

---

## Key Columns & Codes

### Product IDs (in volumetrics)
| Code Pattern | Product |
|---|---|
| `%OIL%`, `%CRD%` | Crude oil |
| `%GAS%` | Natural gas |
| `%CND%`, `%COND%` | Condensate |
| `%WTR%`, `%WATER%` | Produced water |

### Activity IDs (in volumetrics)
| Code | Activity |
|---|---|
| `PROD` | Production |
| `DISP` | Disposition (sent out) |
| `REC` | Received |
| `FUEL` | Fuel gas consumed |
| `VENT` | Vented gas |
| `INJ` | Injection |
| `FLARE` | Flared gas |

When aggregating total production, filter to `ActivityID = 'PROD'` to avoid double-counting.

### Unit Conversions (Petrinex reports in metric)
| Petrinex Unit | O&G Standard | Conversion |
|---|---|---|
| m3 (oil/condensate/water) | barrels (bbl) | x 6.2898 |
| e3m3 (gas) | MCF | x 35.3147 |
| m3/month | bbl/d | x 6.2898 / 30.44 |
| e3m3/month | MCF/d | x 35.3147 / 30.44 |

---

## Repo Structure

```
petrinex_data/
├── README.md
├── setup_data.py           # Databricks notebook -- creates all 8 tables
├── databricks.yml          # Bundle config (catalog/schema variables)
├── resources/
│   └── setup_data_job.yml  # Job definition for bundle deploy
└── data/                   # Pre-built parquet files (~244 MB)
    ├── volumetrics/
    ├── ngl_volumes/
    ├── facilities/
    ├── operators/
    ├── wells/
    ├── field_codes/
    ├── facility_emissions/
    └── market_prices/
```

---

## Data Sources

- **Petrinex Public Data** -- [petrinex.ca](https://www.petrinex.ca/PD/Pages/default.aspx)
- **Market Prices** -- EIA (WTI), Alberta Government (WCS, AECO), Bank of Canada (USD/CAD)
- **Field Codes** -- Alberta Energy Regulator field code registry

## Requirements

- Databricks workspace with Unity Catalog
- Serverless compute or any cluster
- Databricks CLI (for parquet upload and optional bundle deploy)
- ~2 minutes runtime

## License

Data sourced from publicly available Petrinex records.
