# Databricks notebook source
# MAGIC %md
# MAGIC # Petrinex Gold Layer -- M&A Deal Screener
# MAGIC
# MAGIC Builds `gold_deal_screener` from silver tables. One row per NGL-active well with
# MAGIC production, economics, emissions, operator size, and a composite `deal_score`.
# MAGIC
# MAGIC **Runtime:** ~1 minute on Serverless Starter Warehouse.

# COMMAND ----------

dbutils.widgets.text("catalog", "shm", "Catalog Name")
dbutils.widgets.text("schema", "petrinex", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Target: {catalog}.{schema}.gold_deal_screener")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build gold_deal_screener
# MAGIC
# MAGIC Composite `deal_score` (0-100):
# MAGIC - 35% liquids cut (condensate+oil bias)
# MAGIC - 25% seller motivation (small operator + declining production)
# MAGIC - 20% production scale
# MAGIC - 20% netback (CAD/boe)
# MAGIC
# MAGIC Netback proxy: WCS 75 CAD/bbl, AECO 2.5 CAD/GJ, 15% royalty assumption, 12 CAD/boe opex.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.gold_deal_screener AS
WITH operator_well_counts AS (
  SELECT operator_baid, COUNT(*) AS operator_well_count
  FROM {catalog}.{schema}.wells
  WHERE operator_baid IS NOT NULL
  GROUP BY operator_baid
),
recent AS (
  SELECT
    WellID,
    AVG(CAST(NULLIF(GasProduction,'') AS DOUBLE))        AS recent_gas_e3m3,
    AVG(CAST(NULLIF(OilProduction,'') AS DOUBLE))        AS recent_oil_m3,
    AVG(CAST(NULLIF(CondensateProduction,'') AS DOUBLE)) AS recent_cond_m3,
    AVG(CAST(NULLIF(WaterProduction,'') AS DOUBLE))      AS recent_water_m3,
    COUNT(*) AS recent_months
  FROM {catalog}.{schema}.ngl_volumes
  WHERE ProductionMonth >= '2025-07'
  GROUP BY WellID
),
early AS (
  SELECT
    WellID,
    AVG(CAST(NULLIF(GasProduction,'') AS DOUBLE))        AS early_gas_e3m3,
    AVG(CAST(NULLIF(OilProduction,'') AS DOUBLE))        AS early_oil_m3,
    AVG(CAST(NULLIF(CondensateProduction,'') AS DOUBLE)) AS early_cond_m3
  FROM {catalog}.{schema}.ngl_volumes
  WHERE ProductionMonth < '2024-07'
  GROUP BY WellID
),
emissions AS (
  SELECT
    facility_id,
    AVG(CASE WHEN production_volume > 0
             THEN total_emissions_volume / production_volume END) AS emissions_intensity
  FROM {catalog}.{schema}.facility_emissions
  GROUP BY facility_id
),
base AS (
  SELECT
    w.well_id,
    w.facility_id,
    w.facility_name,
    w.operator_baid,
    w.operator_name,
    w.field_display_name,
    w.pool_name,
    w.area_name,
    w.formation,
    w.first_production_month,
    w.last_production_month,
    w.active_months,
    f.latitude,
    f.longitude,
    f.region,
    f.facility_type,
    w.total_oil_m3,
    w.total_gas_e3m3,
    w.total_condensate_m3,
    w.total_water_m3,
    ROUND((COALESCE(w.total_oil_m3,0) + COALESCE(w.total_condensate_m3,0)) * 6.293
          + COALESCE(w.total_gas_e3m3,0) * 5.886, 1) AS cum_boe,
    r.recent_oil_m3,
    r.recent_gas_e3m3,
    r.recent_cond_m3,
    r.recent_water_m3,
    ROUND((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293
          + COALESCE(r.recent_gas_e3m3,0) * 5.886, 1) AS recent_boe_monthly,
    ROUND(((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293
          + COALESCE(r.recent_gas_e3m3,0) * 5.886) / 30.4, 1) AS recent_boe_per_day,
    CASE WHEN ((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293
               + COALESCE(r.recent_gas_e3m3,0) * 5.886) > 0
         THEN ROUND(((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293)
              / ((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293
                 + COALESCE(r.recent_gas_e3m3,0) * 5.886), 3)
         ELSE NULL
    END AS liquids_cut,
    CASE WHEN (COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)
               + COALESCE(r.recent_water_m3,0)) > 0
         THEN ROUND(COALESCE(r.recent_water_m3,0) /
              (COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)
               + COALESCE(r.recent_water_m3,0)), 3)
         ELSE NULL
    END AS water_cut,
    CASE WHEN e.early_oil_m3 IS NOT NULL AND
              ((COALESCE(e.early_oil_m3,0) + COALESCE(e.early_cond_m3,0)) * 6.293
                + COALESCE(e.early_gas_e3m3,0) * 5.886) > 0
         THEN ROUND((
           ((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293
             + COALESCE(r.recent_gas_e3m3,0) * 5.886)
           / ((COALESCE(e.early_oil_m3,0) + COALESCE(e.early_cond_m3,0)) * 6.293
              + COALESCE(e.early_gas_e3m3,0) * 5.886) - 1), 3)
         ELSE NULL
    END AS production_trend_pct,
    em.emissions_intensity,
    ow.operator_well_count,
    CASE
      WHEN ow.operator_well_count < 30  THEN 'Micro (<30)'
      WHEN ow.operator_well_count < 100 THEN 'Small (30-99)'
      WHEN ow.operator_well_count < 500 THEN 'Mid (100-499)'
      ELSE 'Large (500+)'
    END AS operator_size_bucket,
    CASE WHEN ((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293
               + COALESCE(r.recent_gas_e3m3,0) * 5.886) > 0
         THEN ROUND(
           (
             (COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293 * 75.0 * 0.85
             + COALESCE(r.recent_gas_e3m3,0) * 87.5 * 0.85
             -
             ((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293
               + COALESCE(r.recent_gas_e3m3,0) * 5.886) * 12
           ) /
           ((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293
            + COALESCE(r.recent_gas_e3m3,0) * 5.886),
           1)
         ELSE NULL
    END AS netback_cad_per_boe
  FROM {catalog}.{schema}.wells w
  JOIN {catalog}.{schema}.facilities f ON w.facility_id = f.facility_id
  JOIN recent r ON w.well_id = r.WellID
  LEFT JOIN early e ON w.well_id = e.WellID
  LEFT JOIN emissions em ON w.facility_id = em.facility_id
  LEFT JOIN operator_well_counts ow ON w.operator_baid = ow.operator_baid
  WHERE w.formation IS NOT NULL
    AND r.recent_months >= 3
    AND ((COALESCE(r.recent_oil_m3,0) + COALESCE(r.recent_cond_m3,0)) * 6.293
          + COALESCE(r.recent_gas_e3m3,0) * 5.886) > 0
    AND f.latitude IS NOT NULL
    AND f.longitude IS NOT NULL
)
SELECT
  *,
  ROUND(
    35.0 * COALESCE(PERCENT_RANK() OVER (ORDER BY liquids_cut), 0)
    + 25.0 * (
        CASE WHEN operator_well_count < 30  AND production_trend_pct < -0.1 THEN 1.00
             WHEN operator_well_count < 30  THEN 0.65
             WHEN operator_well_count < 100 AND production_trend_pct < -0.1 THEN 0.55
             WHEN operator_well_count < 100 THEN 0.35
             WHEN production_trend_pct < -0.1 THEN 0.20
             ELSE 0.05
        END
      )
    + 20.0 * COALESCE(PERCENT_RANK() OVER (ORDER BY recent_boe_per_day), 0)
    + 20.0 * COALESCE(PERCENT_RANK() OVER (ORDER BY netback_cad_per_boe), 0)
  , 1) AS deal_score
FROM base
""")

count = spark.table(f"{catalog}.{schema}.gold_deal_screener").count()
print(f"gold_deal_screener: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check: top 10 deals

# COMMAND ----------

display(spark.sql(f"""
  SELECT deal_score, operator_name, formation, region, operator_size_bucket,
         recent_boe_per_day, liquids_cut, production_trend_pct, netback_cad_per_boe
  FROM {catalog}.{schema}.gold_deal_screener
  ORDER BY deal_score DESC
  LIMIT 10
"""))
