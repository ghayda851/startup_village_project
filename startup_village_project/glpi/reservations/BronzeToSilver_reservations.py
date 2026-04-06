# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

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


bronze_path = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi/"
silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/reservations"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze Reservation

# COMMAND ----------

df_res_b = spark.read.format("delta").load(f'{bronze_path}/reservations')

# COMMAND ----------

df_res_b.limit(10).display()

# COMMAND ----------

print(df_res_b.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning And Transformation 

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

    # ---------- duration (hours) ----------
    .withColumn(
        "duration_hours",
        when(col("start_ts").isNull() | col("end_ts").isNull(), lit(None).cast("double"))
        .otherwise((unix_timestamp(col("end_ts")) - unix_timestamp(col("start_ts"))) / lit(3600.0))
    )

    # ---------- comment ----------
    .withColumn("comment", clean_str("comment"))

    # ---------- metadata ----------
    .withColumn("ingestion_date", col("ingestion_date").cast("date"))
    .withColumn("_ingest_silver_ts", current_timestamp())
)


# COMMAND ----------

df_res_s = df_res_s.select(
    "reservation_id",
    "user_id",
    "reservation_item_id",
    "group_id",
    "start_ts",
    "end_ts",
    "duration_hours",
    "comment",
    "ingestion_date",
    "_source_file",
    "_source_system",
    "_ingest_bronze_ts",
    "_ingest_silver_ts"
)

# COMMAND ----------

df_res_s.limit(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing To Silver

# COMMAND ----------

# ---- dedup latest per reservation_id (current snapshot) ----
w = Window.partitionBy("reservation_id").orderBy(
    col("_ingest_bronze_ts").desc_nulls_last(),
    col("ingestion_date").desc_nulls_last(),
)
df_res_latest = (
    df_res_s
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

(df_res_latest.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(silver_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis

# COMMAND ----------

strart_ts = (
    df_res_latest
    .where(col("start_ts").isNotNull())
    .groupBy(to_date(col("start_ts")).alias("day"))
    .agg(count("*").alias("room_reserved"))
    .orderBy(col("day"))
)

display(strart_ts)

# COMMAND ----------

item_res = (
    df_res_latest
    .where(col("reservation_item_id").isNotNull())
    .groupBy(col("reservation_item_id").alias("item"))
    .agg(count("*").alias("room_reserved"))
    .orderBy(col("item"))
)

display(item_res)