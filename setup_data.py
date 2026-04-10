# Databricks notebook source
# MAGIC %md
# MAGIC # Petrinex Data Setup
# MAGIC
# MAGIC Creates tables from pre-built parquet files.
# MAGIC Source: [Petrinex Public Data](https://www.petrinex.ca/PD/Pages/default.aspx) (2024-2025).
# MAGIC
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `volumetrics` | ~13M rows -- facility-level monthly production volumes |
# MAGIC | `ngl_volumes` | ~2.5M rows -- well-level NGL production |
# MAGIC | `facilities` | ~25K facilities with lat/lon coordinates |
# MAGIC | `operators` | ~500 operators with production stats |
# MAGIC | `wells` | ~118K wells with formation and field names |
# MAGIC | `field_codes` | 84 AER field code-to-name mappings |
# MAGIC | `facility_emissions` | ~367K flaring/venting/fuel records |
# MAGIC | `market_prices` | 14 months of WTI, WCS, AECO prices |
# MAGIC
# MAGIC **Runtime:** ~2 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "shm", "Catalog Name")
dbutils.widgets.text("schema", "petrinex", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Target: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalog and Schema

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    print(f"Catalog '{catalog}' ready.")
except Exception as e:
    print(f"Catalog note: {e}")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.dataset")

try:
    spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`")
    print("Granted ALL PRIVILEGES to account users.")
except Exception as e:
    print(f"Grant note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Copy Parquet to Volume

# COMMAND ----------

import os, shutil

tables = [
    "volumetrics", "ngl_volumes", "facilities", "operators",
    "wells", "field_codes", "facility_emissions", "market_prices",
]

vol_base = f"/Volumes/{catalog}/{schema}/dataset/parquet"
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_data = "/Workspace" + os.path.dirname(notebook_path) + "/data"

for table in tables:
    src = os.path.join(repo_data, table)
    dst = os.path.join(vol_base, table)
    if not os.path.exists(src):
        print(f"  SKIP {table}: {src} not found (parquet must already be in volume)")
        continue
    if os.path.exists(dst):
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    n = len([f for f in os.listdir(dst) if f.endswith(".parquet")])
    print(f"  {table}: {n} file(s) -> {dst}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Tables

# COMMAND ----------

for table in tables:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.{table}
        AS SELECT * FROM read_files('{vol_base}/{table}/', format => 'parquet')
    """)
    count = spark.table(f"{catalog}.{schema}.{table}").count()
    print(f"  {table:25s} {count:>12,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"{'='*60}")
print(f"  DATA SETUP COMPLETE: {catalog}.{schema}")
print(f"{'='*60}\n")

for t in tables:
    count = spark.table(f"{catalog}.{schema}.{t}").count()
    print(f"  {t:25s} {count:>12,} rows")

print(f"\n  Parquet: {vol_base}/")
print(f"\n  All data from Petrinex Public Data (2024-2025).")
print(f"{'='*60}")
