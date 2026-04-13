# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # GLPI Tickets — Landing to Bronze (incremental by ingestion_date)
# MAGIC
# MAGIC ## What this notebook does
# MAGIC  - Authenticates to ADLS Gen2 using secrets from Azure Key Vault (Databricks secret scope)
# MAGIC  - Finds `ingestion_date=YYYY-MM-DD` partitions that exist in **Landing** but not yet in **Bronze**
# MAGIC    - For each missing partition:
# MAGIC      - Reads only that partition from Landing
# MAGIC      - Adds metadata columns
# MAGIC      - Writes to Bronze Delta partitioned by `ingestion_date` using **overwrite-per-partition** (idempotent)
# MAGIC
# MAGIC  Assumption: Landing ingestion is **strictly new partitions only** (never rewrites an old ingestion_date).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import re

# COMMAND ----------

storage_account = "startupvillagedatalake"
scope_name = "kv-startupvillage"   

client_id = dbutils.secrets.get(scope=scope_name, key="sp-client-id")
client_secret = dbutils.secrets.get(scope=scope_name, key="sp-client-secret")
tenant_id = dbutils.secrets.get(scope=scope_name, key="tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

# COMMAND ----------

dbutils.fs.ls("abfss://landing@startupvillagedatalake.dfs.core.windows.net/glpi/tickets/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paths

# COMMAND ----------

landing_root = "abfss://landing@startupvillagedatalake.dfs.core.windows.net/glpi"
bronze_root = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi"

entity = "tickets"

landing_entity_root = f"{landing_root}/{entity}"
bronze_entity_root = f"{bronze_root}/{entity}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers: list ingestion_date partitions

# COMMAND ----------

def list_ingestion_dates(path: str) -> set[str]:
    """
    Lists partitions like .../ingestion_date=YYYY-MM-DD/ under `path`.
    Returns a set of 'YYYY-MM-DD' strings.
    """
    dates = set()
    for fi in dbutils.fs.ls(path):
        m = re.search(r"ingestion_date=([0-9]{4}-[0-9]{2}-[0-9]{2})", fi.path)
        if m:
            dates.add(m.group(1))
    return dates

def get_missing_dates(landing_path: str, bronze_path: str) -> list[str]:
    landing_dates = list_ingestion_dates(landing_path)

    try:
        bronze_dates = list_ingestion_dates(bronze_path)
    except Exception:
        # First run: bronze path may not exist yet
        bronze_dates = set()

    missing = sorted(landing_dates - bronze_dates)  # ascending => older first
    return missing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determine partitions to process

# COMMAND ----------

missing_dates = get_missing_dates(landing_entity_root, bronze_entity_root)

print(f"[{entity}] Landing partitions:", sorted(list_ingestion_dates(landing_entity_root)))
print(f"[{entity}] Bronze partitions:", sorted(list_ingestion_dates(bronze_entity_root)) if dbutils.fs.ls(bronze_root) else [])
print(f"[{entity}] Missing partitions to process:", missing_dates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process missing partitions (idempotent per partition)

# COMMAND ----------

if not missing_dates:
    print(f"[{entity}] No new ingestion_date partitions to process. Exiting.")
else:
    for d in missing_dates:
        src = f"{landing_entity_root}/ingestion_date={d}/"
        print(f"[{entity}] Processing ingestion_date={d} from {src}")

        df = (
            spark.read.option("multiline", "false")
            .json(src)
            .withColumn("ingestion_date", lit(d).cast("date"))
            .withColumn("_source_file", input_file_name())
            .withColumn("_source_system", lit("glpi"))
            .withColumn("_ingest_bronze_ts", current_timestamp())
        )

        # Idempotent write: overwrite only this partition
        #(
        #    df.write.format("delta")
        #    .mode("overwrite")
        #    .option("replaceWhere", f"ingestion_date = '{d}'")
        #    .partitionBy("ingestion_date")
        #    .save(bronze_entity_root)
        #)

        #print(f"[{entity}] Wrote {df.count()} rows to Bronze for ingestion_date={d}")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick validation

# COMMAND ----------

try:
    print("Bronze tickets partitions:", sorted(list_ingestion_dates(bronze_entity_root)))
except Exception as e:
    print("Could not list Bronze partitions yet:", e)