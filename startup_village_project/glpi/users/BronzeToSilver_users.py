# Databricks notebook source
# MAGIC %md
# MAGIC # GLPI Users — Bronze to Silver (snapshot-based)
# MAGIC
# MAGIC Assumption:
# MAGIC - Each `ingestion_date` partition in Bronze is a full snapshot export.
# MAGIC
# MAGIC What this notebook does:
# MAGIC - Authenticates to ADLS Gen2 using Key Vault secrets
# MAGIC - Reads ONLY the latest ingestion_date partition from Bronze
# MAGIC - Cleans and standardizes user fields (user_id, user_name, full_name, timestamps)
# MAGIC - Keeps 1 row per user_id (latest) as a safety measure
# MAGIC - Writes Silver snapshot with overwrite

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
silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/users"

bronze_users_root = f"{bronze_root}/users"

print("Bronze users:", bronze_users_root)
print("Silver users:", silver_path)

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

latest_date = list_ingestion_dates(bronze_users_root)[-1]
src = f"{bronze_users_root}/ingestion_date={latest_date}/"
print("Reading latest snapshot only:", src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze (latest snapshot only)

# COMMAND ----------

df_user_b = spark.read.format("delta").load(src)

# COMMAND ----------

print("Bronze latest partition rows:", df_user_b.count())
print("Bronze columns:", df_user_b.columns)

# COMMAND ----------

df_user_b.display()

# COMMAND ----------

print(df_user_b.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform to Silver

# COMMAND ----------

def clean_str(cname: str):
    c = col(cname)
    return when(c.isNull() | (trim(c) == ""), lit(None)).otherwise(trim(c))

def to_long(cname: str):
    c = when(col(cname).isNull(), lit(None)).otherwise(trim(col(cname).cast("string")))
    c = when(lower(c) == "null", lit(None)).otherwise(c)
    return c.cast("long")

def to_int(cname: str):
    c = when(col(cname).isNull(), lit(None)).otherwise(trim(col(cname).cast("string")))
    c = when(lower(c) == "null", lit(None)).otherwise(c)
    c = regexp_replace(c, ",", ".")
    return c.cast("int")

df_user_s = (
    df_user_b
    .withColumn("user_id", to_long("id"))
    .withColumn("user_name", clean_str("name"))

    # base before @, then split by dot (dot is regex so must be escaped)
    .withColumn("base_name", split(col("user_name"), "@").getItem(0))
    .withColumn("first_name_raw", split(col("base_name"), r"\.").getItem(0))
    .withColumn("last_name", split(col("base_name"), r"\.").getItem(1))
    .withColumn("first_name", coalesce(col("first_name_raw"), col("base_name")))

    .withColumn(
        "full_name",
        when(col("last_name").isNotNull() & col("first_name").isNotNull(),
             concat(upper(col("last_name")), lit(" "), initcap(col("first_name"))))
        .when(col("last_name").isNotNull(), upper(col("last_name")))
        .when(col("first_name").isNotNull(), initcap(col("first_name")))
        .otherwise(col("user_name"))
    )

    .withColumn("is_active", to_int("is_active"))
    .withColumn("is_deleted", to_int("is_deleted"))
    .withColumn("is_deleted_ldap", to_int("is_deleted_ldap"))

    .withColumn("created_ts", to_timestamp(col("date_creation"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updated_ts", to_timestamp(col("date_mod"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("last_login_ts", to_timestamp(col("last_login"), "yyyy-MM-dd HH:mm:ss"))

    .withColumn("ingestion_date", col("ingestion_date").cast("date"))
    .withColumn("_ingest_silver_ts", current_timestamp())
)

# COMMAND ----------

df_user_s = df_user_s.select(
    "user_id",
    "user_name",
    "first_name",
    "last_name",
    "full_name",
    "is_active",
    "is_deleted",
    "is_deleted_ldap",
    "created_ts",
    "updated_ts",
    "last_login_ts",
    "ingestion_date",
    "_source_file",
    "_source_system",
    "_ingest_bronze_ts",
    "_ingest_silver_ts",
)

# COMMAND ----------

df_user_s.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keep 1 row per user_id (safety dedup)

# COMMAND ----------

w = Window.partitionBy("user_id").orderBy(
    col("updated_ts").desc_nulls_last(),
    col("_ingest_bronze_ts").desc_nulls_last(),
    col("ingestion_date").desc_nulls_last(),
)

# COMMAND ----------

df_user_latest = (
    df_user_s
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

print("Silver users rows:", df_user_latest.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing To Silver

# COMMAND ----------

(df_user_latest.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(silver_path)
)

# COMMAND ----------

df_user_new = spark.read.format("delta").load(f'{silver_path}')

# COMMAND ----------

df_user_new.display()