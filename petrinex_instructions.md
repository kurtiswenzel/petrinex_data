# Petrinex Data Reference

## Coding Conventions

- Use snake_case for variables and function names.
- Prefer PySpark DataFrames over SQL for complex transformations.
- Use Plotly for visualizations unless otherwise specified.
- Include comments in generated code for non-trivial logic only.
- When uncertain, ask clarifying questions before writing complex pipelines.

## Data Location

All tables are in Unity Catalog. The default location is `shm.petrinex`. Adjust catalog and schema as needed.

## Tables and Schemas

### volumetrics (~13M rows)

Facility-level monthly production volumes from Petrinex Conventional Volumetrics reports. Every barrel of oil, MCF of gas, and m3 of water produced, flared, vented, injected, or consumed as fuel at every reporting facility in Alberta.

| Column | Type | Description |
|--------|------|-------------|
| ProductionMonth | STRING | YYYY-MM format |
| OperatorBAID | STRING | Operator Business Associate ID |
| OperatorName | STRING | Legal operator name |
| ReportingFacilityID | STRING | Unique facility identifier |
| ReportingFacilityProvinceState | STRING | Province code (AB) |
| ReportingFacilityType | STRING | BT=Battery, GP=Gas Plant, GS=Gas Gathering, IF=Injection |
| ReportingFacilityIdentifier | STRING | Facility identifier number |
| ReportingFacilityName | STRING | Facility name |
| ReportingFacilitySubType | STRING | Facility sub-type code |
| ReportingFacilitySubTypeDesc | STRING | Sub-type description |
| ReportingFacilityLocation | STRING | DLS location string |
| FacilityLegalSubdivision | STRING | Legal subdivision (1-16) |
| FacilitySection | STRING | Section (1-36) |
| FacilityTownship | STRING | Township number |
| FacilityRange | STRING | Range number |
| FacilityMeridian | STRING | Meridian (1-6, typically 4-6 in Alberta) |
| SubmissionDate | STRING | Date data was submitted |
| ActivityID | STRING | PROD, DISP, REC, FUEL, VENT, INJ, FLARE |
| ProductID | STRING | Product code (contains OIL, GAS, CND, WTR, etc.) |
| FromToID | STRING | Counterparty facility for transfers |
| FromToIDProvinceState | STRING | Counterparty province |
| FromToIDType | STRING | Counterparty facility type |
| FromToIDIdentifier | STRING | Counterparty identifier |
| Volume | DOUBLE | Volume in metric units (m3 for liquids, e3m3 for gas) |
| Energy | DOUBLE | Energy content in GJ |
| Hours | DOUBLE | Operating hours |
| CCICode | STRING | CCI classification code |
| ProrationProduct | STRING | Proration product code |
| ProrationFactor | DOUBLE | Proration factor |
| Heat | DOUBLE | Heat value |

**Critical rule:** When aggregating total production, always filter `ActivityID = 'PROD'`. Other activities (DISP, REC, FUEL, VENT, INJ, FLARE) represent transfers and emissions that will double-count if included.

### ngl_volumes (~2.5M rows)

Well-level monthly NGL (Natural Gas Liquids) production from Petrinex NGL Marketable Gas Volumes reports. Includes gas, oil, condensate, water, and individual NGL component volumes (ethane, propane, butane, pentane).

| Column | Type | Description |
|--------|------|-------------|
| ReportingFacilityID | STRING | Parent facility |
| ReportingFacilityName | STRING | Facility name |
| OperatorBAID | STRING | Operator Business Associate ID |
| OperatorName | STRING | Legal operator name |
| ProductionMonth | STRING | YYYY-MM format |
| WellID | STRING | Unique well identifier (e.g. ABWI...) |
| WellLicenseNumber | STRING | AER well license number |
| Field | STRING | AER field code (numeric, maps to field_codes) |
| Pool | STRING | AER pool code (first 4 digits indicate formation) |
| Area | STRING | Area code |
| Hours | DOUBLE | Producing hours |
| GasProduction | DOUBLE | Raw gas production (e3m3) |
| OilProduction | DOUBLE | Oil production (m3) |
| CondensateProduction | DOUBLE | Condensate production (m3) |
| WaterProduction | DOUBLE | Water production (m3) |
| ResidueGasVolume | DOUBLE | Residue (sales) gas after processing (e3m3) |
| Energy | DOUBLE | Energy content (GJ) |
| EthaneMixVolume | DOUBLE | Ethane in mix stream (m3) |
| EthaneSpecVolume | DOUBLE | Spec ethane (m3) |
| PropaneMixVolume | DOUBLE | Propane in mix stream (m3) |
| PropaneSpecVolume | DOUBLE | Spec propane (m3) |
| ButaneMixVolume | DOUBLE | Butane in mix stream (m3) |
| ButaneSpecVolume | DOUBLE | Spec butane (m3) |
| PentaneMixVolume | DOUBLE | Pentane+ in mix stream (m3) |
| PentaneSpecVolume | DOUBLE | Spec pentane+ (m3) |
| LiteMixVolume | DOUBLE | Light ends in mix stream (m3) |

**Mix vs Spec volumes:** "Mix" volumes are NGL components still in a mixed stream (not yet fractionated). "Spec" volumes are pipeline-quality separated products. Total NGL component = Mix + Spec for each component.

### facilities (~25K rows)

Derived from volumetrics. Unique facilities with DLS-to-lat/lon coordinate conversion.

| Column | Type | Description |
|--------|------|-------------|
| facility_id | STRING | Matches volumetrics.ReportingFacilityID |
| facility_name | STRING | Facility name |
| facility_type_code | STRING | BT, GP, GS, IF |
| facility_subtype | STRING | Sub-type description |
| operator_baid | STRING | Operator ID |
| operator_name | STRING | Operator name |
| township, range_num, meridian, section, lsd | STRING | DLS coordinates |
| dls_location | STRING | Full DLS string |
| latitude | DOUBLE | WGS84 latitude (approximate) |
| longitude | DOUBLE | WGS84 longitude (approximate) |
| region | STRING | Peace Country, Athabasca, West-Central, Central, Foothills, Southeast |
| facility_type | STRING | Battery, Gas Plant, Gas Gathering System, Injection Facility, Other |

### wells (~118K rows)

Derived from ngl_volumes. Per-well production summary with geological formation classification.

| Column | Type | Description |
|--------|------|-------------|
| well_id | STRING | Matches ngl_volumes.WellID |
| facility_id | STRING | Parent facility |
| operator_baid | STRING | Operator ID |
| field_name | STRING | AER field code |
| pool_name | STRING | AER pool code |
| formation | STRING | Geological formation (Montney, Mannville, Cardium, etc.) |
| field_display_name | STRING | Human-readable field name |
| total_gas_e3m3 | DOUBLE | Cumulative gas production |
| total_oil_m3 | DOUBLE | Cumulative oil production |
| total_condensate_m3 | DOUBLE | Cumulative condensate |
| total_water_m3 | DOUBLE | Cumulative water |
| total_ethane_mix_m3 through total_pentane_spec_m3 | DOUBLE | Cumulative NGL components |
| first_production_month, last_production_month | STRING | Production date range |

### operators (~500 rows), field_codes (84 rows), facility_emissions (~367K rows), market_prices (14 rows)

See the README for details on these supporting tables.

## Key Relationships

```
volumetrics.ReportingFacilityID  =  facilities.facility_id
volumetrics.OperatorBAID         =  operators.operator_baid
ngl_volumes.WellID               =  wells.well_id
wells.facility_id                =  facilities.facility_id
wells.field_name                 =  field_codes.field_code
facility_emissions.facility_id   =  facilities.facility_id
```

## Unit Conversions

Petrinex reports in metric. Standard O&G conversions:

| From | To | Multiply by |
|------|----|-------------|
| m3 (oil/condensate/water) | barrels (bbl) | 6.2898 |
| e3m3 (gas) | MCF (thousand cubic feet) | 35.3147 |
| m3/month -> bbl/day | bbl/d | 6.2898 / 30.44 |
| e3m3/month -> MCF/day | MCF/d | 35.3147 / 30.44 |
| m3 oil -> BOE (barrel of oil equivalent) | BOE | 6.2898 |
| e3m3 gas -> BOE | BOE | 6.2898 / 5.6146 (i.e. 6:1 gas-to-oil) |

## Oil Equivalent Conversions

For MRF royalty calculations, oil equivalent volumes (m3e) are used:

| Product | m3e conversion |
|---------|---------------|
| Oil, Condensate, Pentane+ | 1 m3 = 1 m3e |
| Propane | 1 m3 = 0.6817 m3e |
| Butane | 1 m3 = 0.7908 m3e |

Gas equivalent volumes (e3m3e) for gas royalties:

| Product | e3m3e conversion |
|---------|-----------------|
| Gas (methane), Ethane | 1 e3m3 = 1 e3m3e |
| Oil, Condensate, Pentane+ | 1 m3 = 5.6146 e3m3e (6:1 ratio) |
| Propane | 1 m3 = 0.003829 e3m3e |
| Butane | 1 m3 = 0.004441 e3m3e |

## NGL Ratio Calculations

NGL ratios express the volume of each NGL component recovered per unit of raw gas processed. They are reported at the well level from `ngl_volumes`.

### Liquid-Gas Ratio (LGR) -- also called "yield"

```python
# m3 of NGL per e3m3 of raw gas
lgr = (condensate + pentane_mix + pentane_spec + butane_mix + butane_spec
       + propane_mix + propane_spec + ethane_mix + ethane_spec) / gas_production
```

### Gas-Oil Ratio (GOR)

```python
# e3m3 of gas per m3 of oil -- classifies wells as oil vs gas
gor = gas_production / oil_production  # e3m3 / m3
```

A well is conventionally classified as:
- **Oil well:** GOR < 1.0 e3m3/m3 (roughly < 5,600 scf/bbl)
- **Gas well:** GOR >= 1.0 e3m3/m3

### Water Cut

```python
water_cut = water_production / (water_production + oil_production)  # fraction
```

### Component Yields (m3/e3m3 of raw gas)

```python
ethane_yield = (ethane_mix + ethane_spec) / gas_production
propane_yield = (propane_mix + propane_spec) / gas_production
butane_yield = (butane_mix + butane_spec) / gas_production
pentane_yield = (pentane_mix + pentane_spec) / gas_production
condensate_yield = condensate_production / gas_production
```

### Shrinkage Factor

The fraction of raw gas that becomes residue (sales) gas after NGL extraction:

```python
shrinkage = residue_gas_volume / gas_production  # typically 0.85-0.95
```

## Common Patterns

### Filter production only (avoid double-counting)

```sql
SELECT * FROM volumetrics WHERE ActivityID = 'PROD'
```

### Monthly well production with NGL components

```sql
SELECT
    WellID,
    ProductionMonth,
    CAST(GasProduction AS DOUBLE) as gas_e3m3,
    CAST(OilProduction AS DOUBLE) as oil_m3,
    CAST(CondensateProduction AS DOUBLE) as cond_m3,
    CAST(PropaneMixVolume AS DOUBLE) + CAST(PropaneSpecVolume AS DOUBLE) as propane_m3,
    CAST(ButaneMixVolume AS DOUBLE) + CAST(ButaneSpecVolume AS DOUBLE) as butane_m3,
    CAST(PentaneMixVolume AS DOUBLE) + CAST(PentaneSpecVolume AS DOUBLE) as pentane_m3,
    CAST(EthaneMixVolume AS DOUBLE) + CAST(EthaneSpecVolume AS DOUBLE) as ethane_m3
FROM ngl_volumes
WHERE WellID IS NOT NULL
```

### Daily production rate

```sql
SELECT
    OperatorName,
    ROUND(SUM(Volume) * 6.2898 / (COUNT(DISTINCT ProductionMonth) * 30.44)) as avg_bbl_per_day
FROM volumetrics
WHERE ActivityID = 'PROD'
  AND (ProductID LIKE '%OIL%' OR ProductID LIKE '%CRD%')
GROUP BY OperatorName
ORDER BY avg_bbl_per_day DESC
```

### Emissions intensity

```sql
SELECT
    e.facility_id,
    f.facility_name,
    f.operator_name,
    SUM(e.flare_volume) as total_flare,
    SUM(e.production_volume) as total_prod,
    CASE WHEN SUM(e.production_volume) > 0
         THEN SUM(e.flare_volume) / SUM(e.production_volume)
         ELSE NULL END as flare_intensity
FROM facility_emissions e
JOIN facilities f ON e.facility_id = f.facility_id
GROUP BY e.facility_id, f.facility_name, f.operator_name
```
