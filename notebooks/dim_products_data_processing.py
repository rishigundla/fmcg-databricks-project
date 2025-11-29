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
                 .load("abfss://fmcg@dataenggadls001.dfs.core.windows.net/sportsbar/products/")

# COMMAND ----------

df_bronze = df_bronze.withColumn("read_timestamp", current_timestamp())
df_bronze = df_bronze.withColumn("file_name", col("_metadata.file_name"))
df_bronze = df_bronze.withColumn("file_size", col("_metadata.file_size"))

# COMMAND ----------

df_bronze.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.01_bronze.sb_dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Processing

# COMMAND ----------

df_silver = spark.read.table("fmcg.01_bronze.sb_dim_products")

# COMMAND ----------

df_silver = df_silver.dropDuplicates()

# COMMAND ----------

df_silver = df_silver.withColumn("variant",regexp_extract(col("product_name"), r"\((\d+.*\w)\)", 1)) \
                     .withColumn("product", regexp_extract(col("product_name"), r"(.*)\(\d+.*\w\)", 1)) \
                     .withColumn("category", initcap(trim(col("category"))))

# COMMAND ----------

df_silver = df_silver.withColumn("product", regexp_replace(col("product"), "(?i)Protien", "Protein"))
df_silver = df_silver.withColumn("category", regexp_replace(col("category"), "(?i)Protien", "Protein"))

# COMMAND ----------

df_silver = df_silver.withColumn("division",when(col("category") == "Energy Bars", "Nutrition Bars") \
                                           .when(col("category") == "Energy Bars", "Nutrition Bars") \
                                           .when(col("category") == "Protein Bars", "Nutrition Bars") \
                                           .when(col("category") == "Granola & Cereals", "Breakfast Foods") \
                                           .when(col("category") == "Recovery Dairy", "Dairy & Recovery") \
                                           .when(col("category") == "Healthy Snacks", "Healthy Snacks") \
                                           .when(col("category") == "Electrolyte Mix", "Hydration & Electrolytes") \
                                           .otherwise("Other"))

# COMMAND ----------

df_silver = df_silver.withColumn("product_id", when(col("product_id").rlike("^[0-9]+$"), col("product_id")) \
                                           .otherwise(lit('99999999')))
df_silver = df_silver.withColumnRenamed("product_id", "product_code")       

# COMMAND ----------

df_silver.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.02_silver.sb_dim_products") 

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Processing

# COMMAND ----------

sportsbar_products = spark.read.table("fmcg.02_silver.sb_dim_products")

# COMMAND ----------

sportsbar_products = sportsbar_products.select('product_code', 'division', 'category', 'product', 'variant')

# COMMAND ----------

atlikon_products = spark.read.table("fmcg.02_silver.at_dim_products")

# COMMAND ----------

dim_products = atlikon_products.union(sportsbar_products)

# COMMAND ----------

dim_products.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.03_gold.dim_products") 