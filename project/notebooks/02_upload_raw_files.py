# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Upload Raw CSV Files to Databricks Volume
# MAGIC
# MAGIC Copies the 29 securities master CSV files to:
# MAGIC `/Volumes/statestreet/securities_master/raw_files/`
# MAGIC
# MAGIC **One-time setup step.** Only run when source files change.
# MAGIC
# MAGIC ## How to upload
# MAGIC Option A — Databricks UI: Files → Upload to Volume → select all 29 CSVs
# MAGIC Option B — CLI (run this notebook): provide the zip file path below

# COMMAND ----------
import os
import shutil
import zipfile
from pathlib import Path

# Databricks widget for zip file location (DBFS path or local driver path)
dbutils.widgets.text("zip_path", "", "Path to securities_master_data.zip (optional)")
zip_path = dbutils.widgets.get("zip_path").strip()

VOLUME_PATH = "/Volumes/statestreet/securities_master/raw_files/"

# COMMAND ----------
# MAGIC %md ## Check existing files in Volume

# COMMAND ----------
existing = dbutils.fs.ls(VOLUME_PATH) if Path(VOLUME_PATH).exists() else []
print(f"Files already in Volume ({len(existing)}):")
for f in existing:
    print(f"  {f.name} ({f.size:,} bytes)")

# COMMAND ----------
# MAGIC %md ## Upload from ZIP (if zip_path provided)

# COMMAND ----------
if zip_path:
    print(f"Extracting: {zip_path}")
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_files = [n for n in z.namelist() if n.endswith('.csv')]
        print(f"Found {len(csv_files)} CSV files in zip")
        for csv_file in csv_files:
            z.extract(csv_file, "/tmp/sml_extract/")
            fname = os.path.basename(csv_file)
            src = f"/tmp/sml_extract/{csv_file}"
            dst = f"{VOLUME_PATH}{fname}"
            shutil.copy2(src, dst)
            print(f"  ✓ {fname}")
    print(f"\n✅ Uploaded {len(csv_files)} CSV files to {VOLUME_PATH}")
else:
    print("No zip_path provided.")
    print("Upload CSV files manually via Databricks UI:")
    print("  Catalog → statestreet → securities_master → raw_files → Upload")

# COMMAND ----------
# MAGIC %md ## Verify Upload

# COMMAND ----------
files = dbutils.fs.ls(VOLUME_PATH)
csv_files = [f for f in files if f.name.endswith('.csv')]
print(f"\nVerification: {len(csv_files)} CSV files in {VOLUME_PATH}")
for f in csv_files:
    print(f"  ✓ {f.name} ({f.size:,} bytes)")

expected = 29
if len(csv_files) >= expected:
    print(f"\n✅ All {expected} CSV files present. Ready for Bronze ingestion.")
else:
    print(f"\n⚠️  Only {len(csv_files)}/{expected} CSV files found.")
    print("   Upload missing files before running 03_bronze_ingest.py")
