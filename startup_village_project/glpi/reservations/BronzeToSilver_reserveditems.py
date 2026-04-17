# Databricks notebook source
# MAGIC %md
# MAGIC # GLPI Reserved Items — Bronze to Silver (snapshot dimension)
# MAGIC
# MAGIC Purpose:
# MAGIC - Read GLPI ReservationItem data from Bronze (ingested with expand_dropdowns=true)
# MAGIC - Keep ONLY the latest ingestion_date partition (daily snapshot)
# MAGIC - Clean types + rename columns so it matches reservations join key: `reservation_item_id`
# MAGIC - Parse useful IDs from `links` (entity_id, room_id)
# MAGIC - Deduplicate by reservation_item_id (keep latest _ingest_bronze_ts)
# MAGIC - Write a Silver snapshot table for downstream Gold joins

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

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paths

# COMMAND ----------

bronze_path = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi/reserveditems"
silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/reserveditems"

print("Bronze:", bronze_path)
print("Silver:", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze (latest ingestion_date only)

# COMMAND ----------

df_b = spark.read.format("delta").load(bronze_path)

latest_date = df_b.select(max(col("ingestion_date")).alias("d")).collect()[0]["d"]
if latest_date is None:
    raise Exception(f"No ingestion_date found in {bronze_path}")

df = df_b.filter(col("ingestion_date") == lit(latest_date))

print("Latest ingestion_date:", latest_date)
print("Bronze rows (latest):", df.count())
print("Bronze columns:", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC  ## Transform (rename + types + parse links)

# COMMAND ----------

df_t = (
    df
    # rename business key to match reservations join key
    .withColumn("reservation_item_id", col("id").cast("int"))
    .withColumn("reservation_item_name", col("items_id").cast("string"))
    .withColumn("entity_name", col("entities_id").cast("string"))
    .withColumn("itemtype", col("itemtype").cast("string"))
    .withColumn("comment", col("comment").cast("string"))

    # flags
    .withColumn("is_active", (col("is_active").cast("int") == lit(1)).cast("boolean"))
    .withColumn("is_deleted", (col("is_deleted").cast("int") == lit(1)).cast("boolean"))
    .withColumn("is_recursive", (col("is_recursive").cast("int") == lit(1)).cast("boolean"))

    # metadata
    .withColumn("ingestion_date", col("ingestion_date").cast("date"))
    .withColumn("_ingest_silver_ts", current_timestamp())

    # ---------------------------------------------------------
    # CLEAN reservation_item_name IN-PLACE (no new columns added)
    # Rule: trailing "(S-4)" / "(S4)" / "(S15)" means "Salle <n>"
    # Output example: "Salle COSY (S-4)" -> "Salle COSY - Salle 4"
    # ---------------------------------------------------------

    # 1) normalize whitespace + dash characters (keep your existing logic)
    .withColumn("reservation_item_name", trim(col("reservation_item_name")))
    .withColumn("reservation_item_name", regexp_replace(col("reservation_item_name"), r"\s+", " "))
    .withColumn("reservation_item_name", regexp_replace(col("reservation_item_name"), r"[–—−]", "-"))
    .withColumn("reservation_item_name", regexp_replace(col("reservation_item_name"), r"(?<=\w)-(?=\w)", " "))

    # 2) If trailing parentheses contain S + optional spaces/dash + digits, convert to " - Salle <digits>"
    .withColumn(
        "reservation_item_name",
        when(
            # detect: "(S 4)" or "(S-4)" or "(S4)" at end
            regexp_extract(upper(col("reservation_item_name")), r"\(\s*S\s*-?\s*\d+\s*\)\s*$", 0) != "",
            concat(
                # base name without the trailing "(...)"
                trim(regexp_replace(col("reservation_item_name"), r"\s*\([^)]+\)\s*$", "")),
                lit(" - Salle "),
                # extract digits only
                regexp_extract(upper(col("reservation_item_name")), r"\(\s*S\s*-?\s*(\d+)\s*\)\s*$", 1)
            )
        ).otherwise(col("reservation_item_name"))
    )

    # 3) final tidy
    .withColumn("reservation_item_name", trim(col("reservation_item_name")))
    .withColumn("reservation_item_name", regexp_replace(col("reservation_item_name"), r"\s+", " "))
)

# optional re-apply your quality check
df_t = df_t.filter(col("reservation_item_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate by reservation_item_id (keep latest ingest)

# COMMAND ----------

w = Window.partitionBy("reservation_item_id").orderBy(col("_ingest_bronze_ts").desc_nulls_last())
df_dedup = (
    df_t
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

print("Silver reserveditems distinct ids:", df_dedup.select(countDistinct("reservation_item_id")).collect()[0][0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select final Silver schema

# COMMAND ----------

df_out = df_dedup.select(
    "reservation_item_id",
    "reservation_item_name",
    "itemtype",
    "is_active",
    "is_deleted",
    "is_recursive",
    "ingestion_date",
    "_source_file",
    "_source_system",
    "_ingest_bronze_ts",
    "_ingest_silver_ts",
)

# COMMAND ----------

df_out.printSchema()

# COMMAND ----------

df_out.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver (overwrite snapshot)

# COMMAND ----------

(df_out.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(silver_path)
)

print("Wrote Silver reserveditems snapshot to:", silver_path, "rows:", df_out.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional validation read-back

# COMMAND ----------

df_check = spark.read.format("delta").load(silver_path)
print("Silver read-back rows:", df_check.count())

# COMMAND ----------

display(df_check)