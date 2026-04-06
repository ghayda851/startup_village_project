# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.startupvillagedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.startupvillagedatalake.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.startupvillagedatalake.dfs.core.windows.net", 'ac803d5b-6251-4f6b-b1f9-2407fb651dd9')
spark.conf.set(f"fs.azure.account.oauth2.client.secret.startupvillagedatalake.dfs.core.windows.net", 'sIp8Q~4XBFI_epy7gnaf9~o_nruwEOt~1n1yzbRo')
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.startupvillagedatalake.dfs.core.windows.net",
               f"https://login.microsoftonline.com/05a451ff-b8ea-44f1-a99f-7704a79c7f37/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi/")

# COMMAND ----------

bronze_path = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi/"
silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/users"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze Tickets

# COMMAND ----------

df_user_b = spark.read.format("delta").load(f'{bronze_path}/users')

# COMMAND ----------

df_user_b.display()

# COMMAND ----------

print(df_user_b.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *

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
# MAGIC # Data Writing To Silver

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