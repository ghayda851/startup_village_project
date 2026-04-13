# Databricks notebook source
# MAGIC %md
# MAGIC # GLPI Tickets — Bronze to Silver (snapshot-based)
# MAGIC
# MAGIC Assumption:
# MAGIC - Each `ingestion_date` partition in Bronze is a full snapshot export.
# MAGIC
# MAGIC What this notebook does:
# MAGIC - Authenticates to ADLS Gen2 using Key Vault secrets
# MAGIC - Reads ONLY the latest ingestion_date partition from Bronze (fast)
# MAGIC - Transforms and cleans core fields into a Silver snapshot table
# MAGIC - Keeps 1 row per ticket_id (safety dedup)
# MAGIC - Overwrites Silver on each run

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

bronze_root = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi"
silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/tickets"

bronze_tickets_root = f"{bronze_root}/tickets"

print("Bronze tickets:", bronze_tickets_root)
print("Silver tickets:", silver_path)

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

latest_date = list_ingestion_dates(bronze_tickets_root)[-1]
src = f"{bronze_tickets_root}/ingestion_date={latest_date}/"
print("Reading latest snapshot only:", src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Tickets (latest snapshot only)

# COMMAND ----------

df_ticket_b = spark.read.format("delta").load(src)

print("Bronze latest partition rows:", df_ticket_b.count())
print("Bronze columns:", df_ticket_b.columns)

# COMMAND ----------

df_ticket_b.display()

# COMMAND ----------

print(df_ticket_b.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation

# COMMAND ----------

def clean_str(cname: str):
    c = col(cname)
    return when(c.isNull() | (trim(c) == ""), lit(None)).otherwise(trim(c))

def to_long(cname: str):
    return col(cname).cast("long")

def to_int(cname: str):
    # handles null/""/"null" text sometimes
    c = when(col(cname).isNull(), lit(None)).otherwise(trim(col(cname)))
    c = when(lower(c) == "null", lit(None)).otherwise(c)
    c = regexp_replace(c, ",", ".")
    return c.cast("int")

def to_double(cname: str):
    c = when(col(cname).isNull(), lit(None)).otherwise(trim(col(cname)))
    c = when(lower(c) == "null", lit(None)).otherwise(c)
    c = regexp_replace(c, ",", ".")
    return c.cast("double")

df_ticket_s = (
    df_ticket_b
    # ---------- ids ----------
    .withColumn("ticket_id", to_long("id"))
    .withColumn("entity_id", to_long("entities_id"))
    .withColumn("category_id", to_long("itilcategories_id"))
    .withColumn("location_id", to_long("locations_id"))
    .withColumn("request_type_id", to_long("requesttypes_id"))
    .withColumn("requester_user_id", to_long("users_id_recipient"))   # demandeur
    .withColumn("last_updater_user_id", to_long("users_id_lastupdater"))

    # ---------- dates/timestamps ----------
    .withColumn("ticket_date_ts", to_timestamp(col("date")))
    .withColumn("created_ts", to_timestamp(col("date_creation")))
    .withColumn("updated_ts", to_timestamp(col("date_mod")))
    .withColumn("solved_ts", to_timestamp(col("solvedate")))
    .withColumn("closed_ts", to_timestamp(col("closedate")))
    .withColumn("waiting_begin_ts", to_timestamp(col("begin_waiting_date")))
    .withColumn("ola_ttr_begin_ts", to_timestamp(col("ola_ttr_begin_date")))

    # ---------- text ----------
    .withColumn("title", clean_str("name"))
    .withColumn("description_html", clean_str("content"))

    # ---------- flags / enums ----------
    .withColumn("status_code", to_int("status"))
    .withColumn("type_code", to_int("type"))
    .withColumn("priority_code", to_int("priority"))
    .withColumn("impact_code", to_int("impact"))
    .withColumn("urgency_code", to_int("urgency"))
    .withColumn("is_deleted", to_int("is_deleted"))

    .withColumn("global_validation_code", to_int("global_validation"))
    .withColumn("validation_percent", to_double("validation_percent"))

    # ---------- raw time fields (typically seconds) ----------
    .withColumn("action_time_sec", to_int("actiontime"))
    .withColumn("waiting_duration_sec", to_int("waiting_duration"))
    .withColumn("ola_waiting_duration_sec", to_int("ola_waiting_duration"))
    .withColumn("sla_waiting_duration_sec", to_int("sla_waiting_duration"))

    .withColumn("time_to_own_sec", to_int("time_to_own"))
    .withColumn("time_to_resolve_sec", to_int("time_to_resolve"))
    .withColumn("internal_time_to_own_sec", to_int("internal_time_to_own"))
    .withColumn("internal_time_to_resolve_sec", to_int("internal_time_to_resolve"))

    .withColumn("take_into_account_delay_sec", to_int("takeintoaccount_delay_stat"))
    .withColumn("solve_delay_sec", to_int("solve_delay_stat"))
    .withColumn("close_delay_sec", to_int("close_delay_stat"))

    # ---------- SLA/OLA ids ----------
    .withColumn("slas_id_tto", to_long("slas_id_tto"))
    .withColumn("slas_id_ttr", to_long("slas_id_ttr"))
    .withColumn("slalevels_id_ttr", to_long("slalevels_id_ttr"))
    .withColumn("olas_id_tto", to_long("olas_id_tto"))
    .withColumn("olas_id_ttr", to_long("olas_id_ttr"))
    .withColumn("olalevels_id_ttr", to_long("olalevels_id_ttr"))

    # ---------- metadata ----------
    .withColumn("ingestion_date", col("ingestion_date").cast("date"))
    .withColumn("_ingest_silver_ts", current_timestamp())
)

# COMMAND ----------

df_ticket_s = df_ticket_s.select(
    col("ticket_id"),
    col("entity_id"),
    col("category_id"),
    col("request_type_id"),
    col("location_id"),
    col("requester_user_id"),
    col("last_updater_user_id"),
    col("title"),
    col("created_ts"),
    col("updated_ts"),
    col("solved_ts"),
    col("closed_ts"),
    col("status_code"),
    col("priority_code"),
    col("type_code"),
    col("is_deleted"),
    col("ingestion_date"),
    col("_source_file"),
    col("_source_system"),
    col("_ingest_bronze_ts"),
    col("_ingest_silver_ts"),
)

# COMMAND ----------

print("Silver tickets rows (pre-dedup):", df_ticket_s.count())

# COMMAND ----------

display(df_ticket_s.limit(20))

# COMMAND ----------

df_ticket_s.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keep 1 row per ticket_id (safety dedup)

# COMMAND ----------

w = Window.partitionBy("ticket_id").orderBy(
    col("updated_ts").desc_nulls_last(),
    col("_ingest_bronze_ts").desc_nulls_last(),
    col("ingestion_date").desc_nulls_last(),
)

# COMMAND ----------

df_ticket_s_latest = (
    df_ticket_s
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

print("Silver tickets rows (dedup):", df_ticket_s_latest.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing To Silver (overwrite snapshot)

# COMMAND ----------

(df_ticket_s_latest.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(silver_path)
)

# COMMAND ----------

df_ticket_new = spark.read.format("delta").load(f'{silver_path}')