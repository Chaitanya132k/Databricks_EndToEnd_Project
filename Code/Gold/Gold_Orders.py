# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## FACT ORDERS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Reading

# COMMAND ----------

df = spark.sql('SELECT * FROM databricks_cat.silver.orders_silver')

# COMMAND ----------

df_dim_cust = spark.sql('SELECT Dim_Cust_Key, customer_id as dim_customer_id FROM databricks_cat.gold.dim_customers')

# COMMAND ----------

df_dim_prod = spark.sql('SELECT product_id as Dim_Prod_Key,product_id as dim_product_id FROM databricks_cat.gold.dim_products')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Table

# COMMAND ----------

df_fact = df.join(df_dim_cust,df['customer_id']==df_dim_cust['dim_customer_id'],'left').join(df_dim_prod,df['product_id']==df_dim_prod['dim_product_id'],'left')
df_fact_new = df_fact.drop('dim_customer_id','dim_product_id','customer_id','product_id')

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert on Fact

# COMMAND ----------

if spark.catalog.tableExists('databricks_cat.gold.fact_orders'):
    dlt_obj = DeltaTable.forName(spark,'databricks_cat.gold.fact_orders')
    dlt_obj.alias('trg').merge(df_fact_new.alias('src'),'trg.order_id=src.order_id AND trg.Dim_Cust_Key=src.Dim_Cust_Key AND trg.Dim_Prod_Key=src.Dim_Prod_Key')\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()
else:
    df_fact_new.write.format('delta')\
    .option('path','abfss://gold@chaitanyadatabricksete.dfs.core.windows.net/fact_orders')\
    .saveAsTable('databricks_cat.gold.fact_orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_cat.gold.fact_orders

# COMMAND ----------

