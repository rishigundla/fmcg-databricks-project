# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Processing

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_bronze = spark.read.format("csv")\
                 .option("header", "true")\
                 .option("inferSchema", "true")\
                 .load("abfss://fmcg@dataenggadls001.dfs.core.windows.net/sportsbar/orders/landing/")

# COMMAND ----------

df_bronze = df_bronze.withColumn("read_timestamp", current_timestamp())
df_bronze = df_bronze.withColumn("file_name", col("_metadata.file_name"))
df_bronze = df_bronze.withColumn("file_size", col("_metadata.file_size"))

# COMMAND ----------

df_bronze.write.format("delta") \
         .mode("append") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.01_bronze.sb_fact_orders")

# COMMAND ----------

landing_path = "abfss://fmcg@dataenggadls001.dfs.core.windows.net/sportsbar/orders/landing/"
processed_path = "abfss://fmcg@dataenggadls001.dfs.core.windows.net/sportsbar/orders/processed/"

# COMMAND ----------

files = dbutils.fs.ls(landing_path)

for file in files:
  dbutils.fs.mv(
      file.path, 
      f"{processed_path}/{file.name}",
      True
      )

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Processing

# COMMAND ----------

df_silver = spark.read.table("fmcg.01_bronze.sb_fact_orders")

# COMMAND ----------

df_silver = df_silver.dropDuplicates()

# COMMAND ----------

df_silver = df_silver.filter(col("order_qty").isNotNull())
df_silver = df_silver.withColumn("customer_id", when(col("customer_id").rlike("^[0-9]+$"), col("customer_id")).otherwise(lit("999999")))

# COMMAND ----------

df_silver = df_silver.withColumn("order_placement_date", regexp_replace(col("order_placement_date"), r"^[A-Za-z]+,\s*", ""))

# COMMAND ----------

df_silver = df_silver.withColumn("order_placement_date",coalesce(
                                                                try_to_date("order_placement_date", "yyyy/MM/dd"),
                                                                try_to_date("order_placement_date", "dd-MM-yyyy"),
                                                                try_to_date("order_placement_date", "dd/MM/yyyy"),
                                                                try_to_date("order_placement_date", "MMMM dd, yyyy")
                                                                )
)

# COMMAND ----------

df_silver = df_silver.withColumn("date", date_trunc("month", "order_placement_date").cast("date"))

# COMMAND ----------

df_silver = df_silver.withColumn("product_id", col("product_id").cast('string'))\
                     .withColumnRenamed("customer_id", "customer_code")\
                     .withColumnRenamed("product_id", "product_code")\
                     .withColumnRenamed("order_qty", "sold_quantity")

# COMMAND ----------

df_silver = df_silver.groupBy("date", "customer_code", "product_code").agg(sum("sold_quantity").alias("sold_quantity")).orderBy("date")

# COMMAND ----------

df_silver.write.format("delta")\
         .mode("overwrite")\
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.02_silver.sb_fact_orders") 

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Processing

# COMMAND ----------

sportsbar_orders = spark.read.table("fmcg.02_silver.sb_fact_orders")

# COMMAND ----------

sportsbar_orders = sportsbar_orders.select('date', 'product_code', 'customer_code', 'sold_quantity')

# COMMAND ----------

atlikon_orders = spark.read.table("fmcg.02_silver.at_fact_orders")

# COMMAND ----------

dim_fact_orders = atlikon_orders.union(sportsbar_orders)

# COMMAND ----------

dim_fact_orders.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.03_gold.fact_orders") 