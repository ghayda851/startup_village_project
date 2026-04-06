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

dbutils.fs.ls("abfss://landing@startupvillagedatalake.dfs.core.windows.net/files/")

# COMMAND ----------

ingestion_date = "2026-03-07"

src = f"abfss://landing@startupvillagedatalake.dfs.core.windows.net/files/space_repartition/ingestion_date={ingestion_date}/"
dst = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/files/space_repartition"

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Rpartition Data 

# COMMAND ----------

df_repartion = spark.read.option("header", "true")\
                        .option("sep", ",")\
                        .option("quote", '"')\
                        .option("escape", '"')\
                        .option("multiLine", "true")\
                        .option("encoding", "UTF-8")\
                        .option("mode", "PERMISSIVE")\
                        .csv(src)

# COMMAND ----------

df_repartion.display()

# COMMAND ----------

df_repartion = (df_repartion
                            .withColumn("_source_file", input_file_name())
                            .withColumn("_source_system", lit("drive_csv"))
                            .withColumn("_entity", lit("space_repartition"))
                            .withColumn("_ingest_ts", current_timestamp())
                            .withColumn("_ingestion_date", lit(ingestion_date).cast("date"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data To Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing repartion Daata

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake does not allow certain characters in column names, and the CSV header contains them.
# MAGIC
# MAGIC we Created a helper to normalize column names
# MAGIC This will:
# MAGIC - lowercase
# MAGIC - remove accents (é → e)
# MAGIC - replace spaces with _
# MAGIC - remove characters like ', ², etc.

# COMMAND ----------

import re
import unicodedata

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

new_cols = [sanitize_col(c) for c in df_repartion.columns]

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

df_repartion_clean = df_repartion.toDF(*final_cols)
print(df_repartion_clean.columns)

# COMMAND ----------

(df_repartion_clean.write
                  .format("delta")
                  .mode("append")
                  .partitionBy("_ingestion_date")
                  .save(dst)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify it wrote correctly

# COMMAND ----------

dbutils.fs.ls(dst)  # should show _delta_log and partition folders
df_check = spark.read.format("delta").load(dst)
print("bronze rows:", df_check.count())
display(df_check)