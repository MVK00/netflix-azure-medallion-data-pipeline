# Databricks notebook source
# MAGIC %md
# MAGIC #Incrementtal Data Loading using AutoLoader
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA netflix_catalog.net_schema;

# COMMAND ----------

checkpoint_location = "abfss://silver@netflixprojectdlansh1.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", checkpoint_location) \
    .load("abfss://raw@netflixprojectdlansh1.dfs.core.windows.net")

# COMMAND ----------

display(df, checkpointLocation=checkpoint_location)

# COMMAND ----------

df.writeStream \
  .format("delta") \
  .option("checkpointLocation", checkpoint_location) \
  .trigger(availableNow=True) \
  .start("abfss://bronze@netflixprojectdlansh1.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

