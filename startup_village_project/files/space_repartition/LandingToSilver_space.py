# Databricks notebook source
# MAGIC %md
# MAGIC # Space repartition — Landing to Bronze (incremental + idempotent)
# MAGIC
# MAGIC ## What this notebook does
# MAGIC - Reads CSV snapshots from Landing partitioned by `_ingestion_date` (folder: `ingestion_date=YYYY-MM-DD`)
# MAGIC - Detects which ingestion_date partitions exist in Landing but not yet in Bronze
# MAGIC - For each missing partition:
# MAGIC   - Reads only that day’s CSV
# MAGIC   - Adds metadata columns
# MAGIC   - Sanitizes column names (Delta-compatible)
# MAGIC   - Writes to Bronze Delta partitioned by `_ingestion_date` using overwrite-per-partition (`replaceWhere`)
# MAGIC
# MAGIC  Safe to rerun.

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import unicodedata

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

dbutils.fs.ls("abfss://bronze@startupvillagedatalake.dfs.core.windows.net/files/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paths

# COMMAND ----------

landing_root = "abfss://landing@startupvillagedatalake.dfs.core.windows.net/files"
bronze_root  = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/files"

entity = "space_repartition"

landing_entity_root = f"{landing_root}/{entity}"
bronze_entity_root  = f"{bronze_root}/{entity}"

print("Landing:", landing_entity_root)
print("Bronze:", bronze_entity_root)

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
        bronze_dates = set()

    return sorted(landing_dates - bronze_dates)

# COMMAND ----------

missing_dates = get_missing_dates(landing_entity_root, bronze_entity_root)
print(f"[{entity}] Missing partitions to process:", missing_dates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: sanitize CSV headers (Delta-compatible)

# COMMAND ----------

def sanitize_col(c: str) -> str:
    # remove accents (NFKD turns é -> e + accent)
    c = unicodedata.normalize("NFKD", c)
    c = "".join(ch for ch in c if not unicodedata.combining(ch))
    c = c.lower().strip()

    # replace common separators with underscore
    c = re.sub(r"[ \-\/]+", "_", c)

    # remove anything not alphanumeric or underscore
    c = re.sub(r"[^a-z0-9_]", "", c)

    # avoid empty names
    if c == "":
        c = "col"

    return c

def sanitize_df_columns(df):
    new_cols = [sanitize_col(c) for c in df.columns]

    # Ensure uniqueness (in case two columns sanitize to same name)
    seen = {}
    final_cols = []
    for c in new_cols:
        if c in seen:
            seen[c] += 1
            final_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            final_cols.append(c)

    return df.toDF(*final_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process missing partitions (idempotent per day)

# COMMAND ----------

if not missing_dates:
    print(f"[{entity}] No new ingestion_date partitions. Exiting.")
else:
    for d in missing_dates:
        src = f"{landing_entity_root}/ingestion_date={d}/"
        print(f"[{entity}] Processing ingestion_date={d} from {src}")

        # Optional: if folder exists but empty, skip
        try:
            files = dbutils.fs.ls(src)
            if len(files) == 0:
                print(f"[{entity}] Empty folder for {d}. Skipping.")
                continue
        except Exception as e:
            print(f"[{entity}] Could not list files for {src}: {e}. Skipping.")
            continue

        df_raw = (
            spark.read.option("header", "true")
            .option("sep", ",")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .csv(src)
        )

        df_raw = (
            df_raw
            .withColumn("_source_file", input_file_name())
            .withColumn("_source_system", lit("drive_csv"))
            .withColumn("_entity", lit(entity))
            .withColumn("_ingest_ts", current_timestamp())
            .withColumn("_ingestion_date", lit(d).cast("date"))
        )

        df_clean = sanitize_df_columns(df_raw)

        # Idempotent write: overwrite only this partition
        (
            df_clean.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_ingestion_date = '{d}'")
            .partitionBy("_ingestion_date")
            .save(bronze_entity_root)
        )

        print(f"[{entity}] Wrote {df_clean.count()} rows to Bronze for ingestion_date={d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick validation

# COMMAND ----------

try:
    print("Bronze partitions:", sorted(list_ingestion_dates(bronze_entity_root)))
except Exception as e:
    print("Could not list Bronze partitions:", e)