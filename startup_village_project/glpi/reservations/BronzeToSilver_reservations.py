# Databricks notebook source
# MAGIC %md
# MAGIC # GLPI Reservations — Bronze to Silver (snapshot-based)
# MAGIC
# MAGIC ## Assumption
# MAGIC Each `ingestion_date` partition in Bronze is a **full snapshot export**.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC - Authenticates to ADLS Gen2 using Key Vault-backed secrets
# MAGIC - Reads **only the latest** `ingestion_date` partition from Bronze (fast)
# MAGIC - Cleans/standardizes types
# MAGIC - Adds data quality handling for invalid durations (`end_ts < start_ts`)
# MAGIC - Produces a Silver "current snapshot" table (1 row per `reservation_id`)
# MAGIC - Overwrites Silver on each run (because it’s a snapshot)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
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

dbutils.fs.ls("abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paths

# COMMAND ----------

bronze_entity_root = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi/reservations"
silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/reservations"

print("Bronze:", bronze_entity_root)
print("Silver:", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: find latest ingestion_date partition in Bronze (folder listing)

# COMMAND ----------

def list_ingestion_dates(path: str) -> list[str]:
    dates = []
    for fi in dbutils.fs.ls(path):
        m = re.search(r"ingestion_date=([0-9]{4}-[0-9]{2}-[0-9]{2})", fi.path)
        if m:
            dates.append(m.group(1))
    if not dates:
        raise Exception(f"No ingestion_date partitions found under {path}")
    return sorted(set(dates))

latest_date = list_ingestion_dates(bronze_entity_root)[-1]
src = f"{bronze_entity_root}/ingestion_date={latest_date}/"
print("Reading latest snapshot only:", src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze (latest snapshot only)

# COMMAND ----------

df_res_b = spark.read.format("delta").load(src)

# COMMAND ----------

# Optional quick checks
print("Bronze rows (latest partition):", df_res_b.count())
print("Bronze columns:", df_res_b.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform to Silver schema + quality rules

# COMMAND ----------

def clean_str(cname: str):
    c = col(cname)
    return when(c.isNull() | (trim(c) == ""), lit(None)).otherwise(trim(c))

df_res_s = (
    df_res_b
    # ---------- IDs ----------
    .withColumn("reservation_id", col("id").cast("long"))
    .withColumn("user_id", col("users_id").cast("long"))
    .withColumn("reservation_item_id", col("reservationitems_id").cast("long"))
    .withColumn("group_id", col("group").cast("long"))

    # ---------- timestamps ----------
    .withColumn("start_ts", to_timestamp(col("begin"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("end_ts", to_timestamp(col("end"), "yyyy-MM-dd HH:mm:ss"))

    # ---------- quality: invalid duration ----------
    .withColumn(
        "is_invalid_duration",
        col("start_ts").isNotNull() & col("end_ts").isNotNull() & (col("end_ts") < col("start_ts"))
    )

    # ---------- duration (hours) ----------
    .withColumn(
        "duration_hours",
        when(col("start_ts").isNull() | col("end_ts").isNull(), lit(None).cast("double"))
        .when(col("end_ts") < col("start_ts"), lit(None).cast("double"))
        .otherwise((unix_timestamp(col("end_ts")) - unix_timestamp(col("start_ts"))) / lit(3600.0))
    )

    # ---------- comment ----------
    .withColumn("comment", clean_str("comment"))

    # ---------- metadata ----------
    .withColumn("ingestion_date", col("ingestion_date").cast("date"))
    .withColumn("_ingest_silver_ts", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Keep 1 row per reservation_id (current snapshot)

# COMMAND ----------

w = Window.partitionBy("reservation_id").orderBy(
    col("ingestion_date").desc_nulls_last(),
    col("_ingest_bronze_ts").desc_nulls_last(),
)

df_res_latest = (
    df_res_s
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final projection

# COMMAND ----------

df_res_out = df_res_latest.select(
    "reservation_id",
    "user_id",
    "reservation_item_id",
    "group_id",
    "start_ts",
    "end_ts",
    "duration_hours",
    "is_invalid_duration",
    "comment",
    "ingestion_date",
    "_source_file",
    "_source_system",
    "_ingest_bronze_ts",
    "_ingest_silver_ts",
)

# COMMAND ----------

print("Silver snapshot rows:", df_res_out.count())
print("Invalid durations:", df_res_out.filter(col("is_invalid_duration") == True).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver (overwrite snapshot)

# COMMAND ----------

(df_res_out.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path)
)

print("Wrote Silver reservations snapshot to:", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional validation read-back

# COMMAND ----------

df_check = spark.read.format("delta").load(silver_path)
print("Silver read-back rows:", df_check.count())

# COMMAND ----------

#display(df_check)

# COMMAND ----------

# MAGIC %md
# MAGIC # Analytrics

# COMMAND ----------

strart_ts = (
    df_res_latest
    .where(col("start_ts").isNotNull())
    .groupBy(to_date(col("start_ts")).alias("day"))
    .agg(count("*").alias("room_reserved"))
    .orderBy(col("day"))
)

#display(strart_ts)

# COMMAND ----------

item_res = (
    df_res_latest
    .where(col("reservation_item_id").isNotNull())
    .groupBy(col("reservation_item_id").alias("item"))
    .agg(count("*").alias("room_reserved"))
    .orderBy(col("item"))
)

#display(item_res)