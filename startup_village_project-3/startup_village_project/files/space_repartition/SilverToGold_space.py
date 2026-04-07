# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access And Configuration

# COMMAND ----------

from pyspark.sql import functions as F
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

spark.conf.set("spark.sql.session.timeZone", "UTC")

silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/space/space_repartition"

gold_root = "abfss://gold@startupvillagedatalake.dfs.core.windows.net/space"

gold_space_room_current_path = f"{gold_root}/space_room_current"
gold_kpi_global_path = f"{gold_root}/kpi_global_current"
gold_kpi_by_site_path = f"{gold_root}/kpi_by_site_current"
gold_kpi_by_site_space_type_path = f"{gold_root}/kpi_by_site_space_type_current"
gold_kpi_by_site_org_type_path = f"{gold_root}/kpi_by_site_org_type_current"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Silver

# COMMAND ----------

df_s = spark.read.format("delta").load(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # NORMALIZE + KEYS

# COMMAND ----------

def norm(c):
    return F.lower(F.trim(F.col(c)))

df = (
    df_s
    .withColumn("site_norm", norm("site"))
    .withColumn("floor_norm", norm("floor"))
    .withColumn("room_norm", norm("room"))
    # natural key for upserts + joins
    .withColumn("room_nk", F.concat_ws("|", F.col("site_norm"), F.col("floor_norm"), F.col("room_norm")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # PICK LATEST RECORD PER ROOM

# COMMAND ----------

# MAGIC %md
# MAGIC ### latest by _ingestion_date then _silver_ts

# COMMAND ----------

w = Window.partitionBy("room_nk").orderBy(F.col("_ingestion_date").desc(), F.col("_silver_ts").desc())

df_latest = (
    df
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # DERIVED METRICS

# COMMAND ----------

df_gold_room_current = (
    df_latest
    .withColumn("employee_count", F.col("employee_count").cast("int"))
    .withColumn("total_capacity", F.col("total_capacity").cast("int"))
    .withColumn("area_m2", F.col("area_m2").cast("double"))
    # keep nulls for unknown area/capacity to avoid wrong averages
    .withColumn(
        "occupancy_rate_pct",
        F.when(F.col("total_capacity").isNull() | (F.col("total_capacity") == 0), F.lit(None).cast("double"))
         .otherwise((F.coalesce(F.col("employee_count"), F.lit(0)) / F.col("total_capacity")) * F.lit(100.0))
    )
    .withColumn(
        "density_emp_per_100m2",
        F.when(F.col("area_m2").isNull() | (F.col("area_m2") <= 0), F.lit(None).cast("double"))
         .otherwise((F.coalesce(F.col("employee_count"), F.lit(0)) / F.col("area_m2")) * F.lit(100.0))
    )
    .withColumn(
        "is_over_capacity",
        F.when(F.col("employee_count").isNull() | F.col("total_capacity").isNull(), F.lit(None).cast("boolean"))
         .otherwise(F.col("employee_count") > F.col("total_capacity"))
    )
    .withColumn("last_updated_ts", F.current_timestamp())
    .select(
        "room_nk",
        "site", "floor", "room",
        "occupancy_status",
        "organization_name", "organization_type",
        "space_type", "activity",
        "employee_count", "total_capacity", "area_m2",
        "occupancy_rate_pct", "density_emp_per_100m2",
        "is_over_capacity",
        "_ingestion_date",
        "_source_file", "_source_system", "_entity",
        "_ingest_ts", "_silver_ts",
        "last_updated_ts"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # WRITE GOLD: ROOM CURRENT

# COMMAND ----------

(df_gold_room_current.write
 .format("delta")
 .mode("overwrite")  # small dataset; cheapest and simplest
 .save(gold_space_room_current_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # KPI: GLOBAL CURRENT

# COMMAND ----------

# MAGIC %md
# MAGIC Weighted occupancy rate: sum(employees)/sum(capacity) * 100

# COMMAND ----------

df_kpi_global = (
    df_gold_room_current
    .agg(
        F.countDistinct("site").alias("villages_count"),
        F.countDistinct("room_nk").alias("total_spaces"),
        F.sum(F.col("area_m2")).alias("total_area_m2"),
        F.sum(F.col("total_capacity")).alias("total_capacity"),
        F.sum(F.col("employee_count")).alias("total_employees"),
        F.sum(F.when(norm("occupancy_status") == "loué", 1).otherwise(0)).alias("leased_spaces"),
        F.sum(F.when(norm("occupancy_status") == "non loué", 1).otherwise(0)).alias("non_leased_spaces"),
        F.sum(F.when(norm("occupancy_status").like("%réser%"), 1).otherwise(0)).alias("reservation_spaces"),
        F.sum(F.when(norm("occupancy_status").like("%personne%"), 1).otherwise(0)).alias("per_person_spaces"),
        F.sum(F.when(F.col("occupancy_rate_pct") < 40, 1).otherwise(0)).alias("underutilized_spaces_lt_40pct"),
        F.sum(F.when(F.col("occupancy_rate_pct") > 100, 1).otherwise(0)).alias("overoccupied_spaces_gt_100pct"),
        F.avg(F.col("density_emp_per_100m2")).alias("avg_density_emp_per_100m2"),
    )
    .withColumn(
        "weighted_occupancy_rate_pct",
        F.when(F.col("total_capacity").isNull() | (F.col("total_capacity") == 0), F.lit(None).cast("double"))
         .otherwise((F.col("total_employees") / F.col("total_capacity")) * F.lit(100.0))
    )
    .withColumn("_as_of_ts", F.current_timestamp())
)

(df_kpi_global.write
 .format("delta")
 .mode("overwrite")
 .save(gold_kpi_global_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # KPI: BY SITE

# COMMAND ----------

df_kpi_by_site = (
    df_gold_room_current
    .groupBy("site")
    .agg(
        F.countDistinct("room_nk").alias("total_spaces"),
        F.sum("area_m2").alias("total_area_m2"),
        F.sum("total_capacity").alias("total_capacity"),
        F.sum("employee_count").alias("total_employees"),
        F.sum(F.when(norm("occupancy_status") == "loué", 1).otherwise(0)).alias("leased_spaces"),
        F.sum(F.when(norm("occupancy_status") == "non loué", 1).otherwise(0)).alias("non_leased_spaces"),
        F.sum(F.when(F.col("occupancy_rate_pct") < 40, 1).otherwise(0)).alias("underutilized_spaces_lt_40pct"),
        F.sum(F.when(F.col("occupancy_rate_pct") > 100, 1).otherwise(0)).alias("overoccupied_spaces_gt_100pct"),
        F.avg(F.col("density_emp_per_100m2")).alias("avg_density_emp_per_100m2"),
    )
    .withColumn(
        "weighted_occupancy_rate_pct",
        F.when(F.col("total_capacity").isNull() | (F.col("total_capacity") == 0), F.lit(None).cast("double"))
         .otherwise((F.col("total_employees") / F.col("total_capacity")) * F.lit(100.0))
    )
    .withColumn("_as_of_ts", F.current_timestamp())
)

(df_kpi_by_site.write
 .format("delta")
 .mode("overwrite")
 .save(gold_kpi_by_site_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # KPI: BY SITE + SPACE TYPE (for surface by type chart)

# COMMAND ----------

df_kpi_by_site_space_type = (
    df_gold_room_current
    .groupBy("site", "space_type")
    .agg(
        F.countDistinct("room_nk").alias("spaces_count"),
        F.sum("area_m2").alias("sum_area_m2"),
        F.sum("total_capacity").alias("sum_capacity"),
        F.sum("employee_count").alias("sum_employees"),
    )
    .withColumn("_as_of_ts", F.current_timestamp())
)

(df_kpi_by_site_space_type.write
 .format("delta")
 .mode("overwrite")
 .save(gold_kpi_by_site_space_type_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # KPI: BY SITE + ORG TYPE (for employees by org type chart)

# COMMAND ----------

df_kpi_by_site_org_type = (
    df_gold_room_current
    .groupBy("site", "organization_type")
    .agg(
        F.countDistinct("room_nk").alias("spaces_count"),
        F.sum("employee_count").alias("sum_employees"),
        F.sum("area_m2").alias("sum_area_m2"),
        F.sum("total_capacity").alias("sum_capacity"),
    )
    .withColumn("_as_of_ts", F.current_timestamp())
)

(df_kpi_by_site_org_type.write
 .format("delta")
 .mode("overwrite")
 .save(gold_kpi_by_site_org_type_path)
)

# COMMAND ----------

gold_root = "abfss://gold@startupvillagedatalake.dfs.core.windows.net/space"

gold_space_room_current_path = f"{gold_root}/space_room_current"
gold_kpi_global_path = f"{gold_root}/kpi_global_current"
gold_kpi_by_site_path = f"{gold_root}/kpi_by_site_current"
gold_kpi_by_site_space_type_path = f"{gold_root}/kpi_by_site_space_type_current"
gold_kpi_by_site_org_type_path = f"{gold_root}/kpi_by_site_org_type_current"

# COMMAND ----------

df_room = spark.read.format("delta").load(gold_space_room_current_path)
df_global = spark.read.format("delta").load(gold_kpi_global_path)
df_site = spark.read.format("delta").load(gold_kpi_by_site_path)
df_site_space_type = spark.read.format("delta").load(gold_kpi_by_site_space_type_path)
df_site_org_type = spark.read.format("delta").load(gold_kpi_by_site_org_type_path)

# COMMAND ----------

df_room.display()

# COMMAND ----------

df_global.display()

# COMMAND ----------

df_site.display()

# COMMAND ----------

df_site_space_type.display()

# COMMAND ----------

df_site_org_type.display()