# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Tables

# COMMAND ----------

#Expectations
my_rules = {
    'rule1':'product_id IS NOT NULL',
    'rule2':'product_name IS NOT NULL'
}

# COMMAND ----------

@dlt.table(name = 'Dim_Products_stage')

@dlt.expect_all_or_drop(my_rules)
def Dim_Products_stage():
    df = spark.readStream.table('databricks_cat.silver.products_silver')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming View

# COMMAND ----------

@dlt.view
def Dim_Products_view():
    df = spark.readStream.table('Live.Dim_Products_stage')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Products

# COMMAND ----------

dlt.create_streaming_table('Dim_Products')

# COMMAND ----------

dlt.apply_changes(
  target = "Dim_Products",
  source = "Live.Dim_Products_view",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type = 2
)

# COMMAND ----------

