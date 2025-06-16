# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Data Reading

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@chaitanyadatabricksete.dfs.core.windows.net/products')

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cat.bronze.discount_func(p_price DOUBLE,discount_val INT)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price - (p_price*(discount_val/100))

# COMMAND ----------

df = df.withColumn('discounted_price',expr('databricks_cat.bronze.discount_func(price,10)'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cat.bronze.upper_func(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS
# MAGIC $$
# MAGIC     return p_brand.upper()
# MAGIC $$

# COMMAND ----------

df.write.format('delta')\
    .mode('overwrite')\
        .option('path','abfss://silver@chaitanyadatabricksete.dfs.core.windows.net/products')\
            .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cat.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@chaitanyadatabricksete.dfs.core.windows.net/products'

# COMMAND ----------

