# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Notebook - Gold Layer

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation Rules

# COMMAND ----------

looktables_rules = {
    "rule1": "show_id IS NOT NULL"
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Directors

# COMMAND ----------

@dlt.table(name="gold_netflix_directors")
@dlt.expect_all_or_drop(looktables_rules)
def gold_netflix_directors():
    return spark.readStream.format("delta").load(
        "abfss://silver@netflixprojectdlansh1.dfs.core.windows.net/netflix_directors"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Cast
# MAGIC

# COMMAND ----------

@dlt.table(name="gold_netflix_cast")
@dlt.expect_all_or_drop(looktables_rules)
def gold_netflix_cast():
    return spark.readStream.format("delta").load(
        "abfss://silver@netflixprojectdlansh1.dfs.core.windows.net/netflix_cast"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Categories
# MAGIC

# COMMAND ----------

@dlt.table(name="gold_netflix_categories")
@dlt.expect_all_or_drop(looktables_rules)
def gold_netflix_categories():
    return spark.readStream.format("delta").load(
        "abfss://silver@netflixprojectdlansh1.dfs.core.windows.net/netflix_category"
    )
 

# COMMAND ----------

# MAGIC %md
# MAGIC # Countries
# MAGIC

# COMMAND ----------

@dlt.table(name="gold_netflix_countries")
@dlt.expect_all_or_drop(looktables_rules)
def gold_netflix_countries():
    return spark.readStream.format("delta").load(
        "abfss://silver@netflixprojectdlansh1.dfs.core.windows.net/netflix_countries"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Stage Titles

# COMMAND ----------

@dlt.table(name="gold_stg_netflix_titles")
def gold_stg_netflix_titles():
    return spark.readStream.format("delta").load(
        "abfss://silver@netflixprojectdlansh1.dfs.core.windows.net/netflix_titles"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform Titles
# MAGIC

# COMMAND ----------

@dlt.table(name="gold_trns_netflix_titles")
def gold_trns_netflix_titles():
    df = spark.readStream.table("LIVE.gold_stg_netflix_titles")
    return df.withColumn("newflag", lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC # Final Gold Titles

# COMMAND ----------

masterdata_rules = {
    "rule1": "newflag IS NOT NULL",
    "rule2": "show_id IS NOT NULL"
}
 

# COMMAND ----------

@dlt.table(name="gold_netflix_titles")
@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflix_titles():
    return spark.readStream.table("LIVE.gold_trns_netflix_titles")