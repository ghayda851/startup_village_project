# Databricks notebook source
# MAGIC %md
# MAGIC # Space repartition — Bronze to Silver (snapshot-based)
# MAGIC
# MAGIC Assumption:
# MAGIC - Each `_ingestion_date` partition in Bronze is a full daily snapshot.
# MAGIC
# MAGIC What this notebook does:
# MAGIC - Reads ONLY the latest `_ingestion_date` partition from Bronze
# MAGIC - Cleans and standardizes columns (site/floor/room/org/space fields)
# MAGIC - Parses numeric columns safely
# MAGIC - Writes a Silver "current snapshot" table (overwrite)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
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

dbutils.fs.ls("abfss://landing@startupvillagedatalake.dfs.core.windows.net/files/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paths

# COMMAND ----------

bronze_path = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/files/space_repartition"
silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/space/space_repartition"

print("Bronze:", bronze_path)
print("Silver:", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: find latest _ingestion_date partition in Bronze (folder listing)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

def list_ingestion_dates(path: str) -> list[str]:
    dates = []
    for fi in dbutils.fs.ls(path):
        m = re.search(r"_ingestion_date=([0-9]{4}-[0-9]{2}-[0-9]{2})", fi.path)
        if m:
            dates.append(m.group(1))
    if not dates:
        raise Exception(f"No _ingestion_date partitions found under {path}")
    return sorted(set(dates))

latest_date = list_ingestion_dates(bronze_path)[-1]
src = f"{bronze_path}/_ingestion_date={latest_date}/"
print("Reading latest snapshot only:", src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze (latest snapshot only)

# COMMAND ----------

df_rep_b = spark.read.format("delta").load(src)
print("Bronze rows (latest):", df_rep_b.count())
print("Bronze columns:", df_rep_b.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning helpers

# COMMAND ----------

def s(cname: str):
    """lower(trim(col)) shortcut used for comparisons + standardized output"""
    return lower(trim(col(cname)))

def is_null_or_dash(cname: str):
    """true if null, empty, or '-' """
    c = col(cname)
    return c.isNull() | (trim(c) == "") | (trim(c) == "-")

def clean_text(cname: str, default_value=None):
    """lower/trim and convert null/''/'-' to default_value (or null)"""
    base = s(cname)
    if default_value is None:
        return when(is_null_or_dash(cname), lit(None)).otherwise(base)
    return when(is_null_or_dash(cname), lit(default_value)).otherwise(base)

def to_int(cname: str):
    """parse int; '9,5' -> 9.5 then cast; null/''/'-' -> null"""
    txt = when(is_null_or_dash(cname), lit(None)).otherwise(trim(col(cname)))
    txt = regexp_replace(txt, ",", ".")
    return txt.cast("int")

def to_double(cname: str):
    """parse double; '9,5' -> 9.5; null/''/'-' -> null"""
    txt = when(is_null_or_dash(cname), lit(None)).otherwise(trim(col(cname)))
    txt = regexp_replace(txt, ",", ".")
    return txt.cast("double")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform to Silver schema

# COMMAND ----------

df_rep_s = (
    df_rep_b
    # English-ish business columns
    .withColumn("site", clean_text("viilage"))  # (typo in original file header, keep as-is)
    .withColumn(
        "floor",
        when(s("etage").like("1èr%"), lit("premier étage"))
        .when(s("etage").like("2ème%"), lit("deuxième étage"))
        .when(s("etage").like("9ème%"), lit("neuvième étage"))
        .when(s("etage").like("rdc%"), lit("rez de chaussée"))
        .otherwise(s("etage"))
    )
    .withColumn(
        "room",
        when(s("salle").like("%01"), lit("salle 01"))
        .when(s("salle").like("%02"), lit("salle 02"))
        .when(s("salle").like("%03"), lit("salle 03"))
        .when(s("salle").like("%04"), lit("salle 04"))
        .when(s("salle").like("%05"), lit("salle 05"))
        .when(s("salle").like("%06"), lit("salle 06"))
        .when(s("salle").like("%07"), lit("salle 07"))
        .when(s("salle").like("%08"), lit("salle 08"))
        .when(s("salle").like("%09"), lit("salle 09"))
        .when(s("salle").like("%10"), lit("salle 10"))
        .when(s("salle").like("%11"), lit("salle 11"))
        .when(s("salle").like("%12"), lit("salle 12"))
        .when(s("salle").like("%13"), lit("salle 13"))
        .when(s("salle").like("%14"), lit("salle 14"))
        .when(s("salle").like("%15"), lit("salle 15"))
        .when(s("salle").like("%16"), lit("salle 16"))
        .when(s("salle").like("%17"), lit("salle 17"))
        .when(s("salle").like("%18"), lit("salle 18"))
        .when(s("salle").like("%19"), lit("salle 19"))
        .when(s("salle").like("%20"), lit("salle 20"))
        .when(s("salle").like("%21"), lit("salle 21"))
        .when(s("salle").like("%22"), lit("salle 22"))
        .when(s("salle").like("%23"), lit("salle 23"))
        .when(s("salle").like("%24"), lit("salle 24"))
        .when(s("salle").like("%open space%"), lit("open space"))
        .when(s("salle").like("%grande terrasse%"), lit("grande terrasse"))
        .when(s("salle").like("%petite terrasse%"), lit("petite terrasse"))
        .when(s("salle").like("%tesrrasse c%"), lit("terrasse"))
        .otherwise(s("salle"))
    )
    .withColumn("occupancy_status", s("etat"))

    .withColumn("organization_name", clean_text("nom_de_lorganisation", default_value="n/a"))
    .withColumn(
        "organization_type",
        when(s("type_de_lorganisation").like("%costorage%"), lit("costorage"))
        .when(is_null_or_dash("type_de_lorganisation"), lit("n/a"))
        .otherwise(s("type_de_lorganisation"))
    )
    .withColumn(
        "space_type",
        when(is_null_or_dash("type_de_lespace"), s("etat"))
        .otherwise(s("type_de_lespace"))
    )
    .withColumn("activity", clean_text("activite", default_value="n/a"))

    # numeric parsing
    .withColumn("employee_count", to_int("nb_employes"))
    .withColumn("total_capacity", to_int("capacite_totale"))
    .withColumn("area_m2", to_double("surface_m2"))  # keep NULL when unknown

    # occupancy rate %
    .withColumn(
        "occupancy_rate_pct",
        when(col("total_capacity").isNull() | (col("total_capacity") == 0), lit(None).cast("double"))
        .otherwise((coalesce(col("employee_count"), lit(0)) / col("total_capacity")) * lit(100.0))
    )

    # metadata
    .withColumn("_ingestion_date", col("_ingestion_date").cast("date"))
    .withColumn("_silver_ts", current_timestamp())
)

# Filter: room must exist
df_rep_s = df_rep_s.filter(col("room").isNotNull())

df_rep_out = df_rep_s.select(
    "site", "floor", "room", "occupancy_status",
    "organization_name", "organization_type",
    "space_type", "activity",
    "employee_count", "total_capacity",
    "area_m2", "occupancy_rate_pct",
    "_ingestion_date",
    "_source_file", "_source_system", "_entity", "_ingest_ts",
    "_silver_ts",
)

print("Silver rows (snapshot):", df_rep_out.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver snapshot (overwrite)

# COMMAND ----------

(df_rep_out.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(silver_path)
)