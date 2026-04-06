# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import re
from datetime import datetime

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.startupvillagedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.startupvillagedatalake.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.startupvillagedatalake.dfs.core.windows.net", 'ac803d5b-6251-4f6b-b1f9-2407fb651dd9')
spark.conf.set(f"fs.azure.account.oauth2.client.secret.startupvillagedatalake.dfs.core.windows.net", 'sIp8Q~4XBFI_epy7gnaf9~o_nruwEOt~1n1yzbRo')
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.startupvillagedatalake.dfs.core.windows.net",
               f"https://login.microsoftonline.com/05a451ff-b8ea-44f1-a99f-7704a79c7f37/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://landing@startupvillagedatalake.dfs.core.windows.net/glpi/")

# COMMAND ----------

dbutils.fs.ls("abfss://landing@startupvillagedatalake.dfs.core.windows.net/glpi/tickets/")

# COMMAND ----------

landing_root = "abfss://landing@startupvillagedatalake.dfs.core.windows.net/glpi"
bronze_root = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi"
entities = ["tickets", "users", "computers", "reservations"]

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Ticket Data

# COMMAND ----------

df = spark.read.option("multiline", "false").json(f"{landing_root}/tickets/")

latest_ingestion_date = df.select(max("ingestion_date")).first()[0]
latest_path = f"{landing_root}/tickets/ingestion_date={latest_ingestion_date}/"

df_ticket = (
    spark.read.option("multiline", "false")
    .json(latest_path)
    .withColumn("ingestion_date", lit(latest_ingestion_date))
    .withColumn("_source_file", input_file_name())
    .withColumn("_source_system", lit("glpi"))
    .withColumn("_ingest_bronze_ts", current_timestamp())
)

print("Reading only:", latest_path)

# COMMAND ----------

display(df_ticket)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Reservation Data

# COMMAND ----------

df_reservation = spark.read \
    .option("multiline", "false")\
    .json(f"{landing_root}/reservations/")\
    .withColumn("_source_file", input_file_name())\
    .withColumn("_source_system", lit("glpi"))\
    .withColumn("_ingest_bronze_ts", current_timestamp())

# COMMAND ----------

display(df_reservation)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read User Data

# COMMAND ----------

df_user = spark.read \
    .option("multiline", "false")\
    .json(f"{landing_root}/users/")\
    .withColumn("_source_file", input_file_name())\
    .withColumn("_source_system", lit("glpi"))\
    .withColumn("_ingest_bronze_ts", current_timestamp())

# COMMAND ----------

display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Computer Data

# COMMAND ----------

df_computer = spark.read \
    .option("multiline", "false")\
    .json(f"{landing_root}/computers/")\
    .withColumn("_source_file", input_file_name())\
    .withColumn("_source_system", lit("glpi"))\
    .withColumn("_ingest_bronze_ts", current_timestamp())

# COMMAND ----------

display(df_computer)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data To Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Tickets Data

# COMMAND ----------

df_ticket.write.format("delta")\
                .mode("append")\
                .partitionBy("ingestion_date")\
                .save(f"{bronze_root}/tickets")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Reservation Data

# COMMAND ----------

df_reservation.write.format("delta")\
                    .mode("append")\
                    .partitionBy("ingestion_date")\
                    .save(f"{bronze_root}/reservations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write User Data

# COMMAND ----------

df_user.write.format("delta")\
                .mode("append")\
                .partitionBy("ingestion_date")\
                .save(f"{bronze_root}/users")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Computer Data

# COMMAND ----------

df_computer.write.format("delta")\
                    .mode("append")\
                    .partitionBy("ingestion_date")\
                    .save(f"{bronze_root}/computers")

# COMMAND ----------

# MAGIC %md
# MAGIC