# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.startupvillagedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.startupvillagedatalake.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.startupvillagedatalake.dfs.core.windows.net", 'ac803d5b-6251-4f6b-b1f9-2407fb651dd9')
spark.conf.set(f"fs.azure.account.oauth2.client.secret.startupvillagedatalake.dfs.core.windows.net", 'sIp8Q~4XBFI_epy7gnaf9~o_nruwEOt~1n1yzbRo')
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.startupvillagedatalake.dfs.core.windows.net",
               f"https://login.microsoftonline.com/05a451ff-b8ea-44f1-a99f-7704a79c7f37/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@startupvillagedatalake.dfs.core.windows.net/files/")

# COMMAND ----------

repartition_bronze = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/files/space_repartition"
silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/space/space_repartition"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze

# COMMAND ----------

df_rep_b = spark.read.format("delta").load(repartition_bronze)

# COMMAND ----------

print(df_rep_b.count())
display(df_rep_b)

# COMMAND ----------

print(spark.read.format("delta").load("abfss://bronze@startupvillagedatalake.dfs.core.windows.net/files/space_repartition").columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning And Transformation 

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
    """parse int; normalize comma decimals first; null/''/'-' -> null"""
    txt = when(is_null_or_dash(cname), lit(None)).otherwise(trim(col(cname)))
    txt = regexp_replace(txt, ",", ".")
    return txt.cast("int")

def to_double(cname: str):
    """parse double; '9,5' -> 9.5; null/''/'-' -> null"""
    txt = when(is_null_or_dash(cname), lit(None)).otherwise(trim(col(cname)))
    txt = regexp_replace(txt, ",", ".")
    return txt.cast("double")

df_rep_silver = (
    df_rep_b
    # English column names (business)
    .withColumn("site", clean_text("viilage"))  # this is the village/site name in your file
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

    # org + space fields
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
    .withColumn("area_m2", coalesce(to_double("surface_m2"), lit(0.0)))

    # occupancy rate %
    .withColumn(
        "occupancy_rate_pct",
        when(col("total_capacity").isNull() | (col("total_capacity") == 0), lit(0.0))
        .otherwise((coalesce(col("employee_count"), lit(0)) / col("total_capacity")) * lit(100.0))
    )

    # metadata
    .withColumn("_ingestion_date", col("_ingestion_date").cast("date"))
    .withColumn("_silver_ts", current_timestamp())
)

# Filter: WHERE salle IS NOT NULL (after standardizing)
df_rep_silver = df_rep_silver.filter(col("room").isNotNull())

# Final projection (English + metadata)
df_rep_silver = df_rep_silver.select(
    "site", "floor", "room", "occupancy_status",
    "organization_name", "organization_type",
    "space_type", "activity",
    "employee_count", "total_capacity",
    "area_m2", "occupancy_rate_pct",
    "_ingestion_date",
    "_source_file", "_source_system", "_entity", "_ingest_ts",
    "_silver_ts"
)

# COMMAND ----------

display(df_rep_silver)

# COMMAND ----------

df_rep_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data To Silver

# COMMAND ----------

dates = [r["_ingestion_date"] for r in df_rep_silver.select("_ingestion_date").distinct().collect()]
replace_where = " OR ".join([f"_ingestion_date = '{d}'" for d in dates])

# COMMAND ----------

(df_rep_silver.write
  .format("delta")
  .mode("overwrite")
  .option("replaceWhere", replace_where)
  .partitionBy("_ingestion_date")
  .save(silver_path)
)