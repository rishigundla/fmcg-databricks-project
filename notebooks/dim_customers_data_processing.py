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
                 .load("abfss://fmcg@dataenggadls001.dfs.core.windows.net/sportsbar/customers/")

# COMMAND ----------

df_bronze = df_bronze.withColumn("read_timestamp", current_timestamp())
df_bronze = df_bronze.withColumn("file_name", col("_metadata.file_name"))
df_bronze = df_bronze.withColumn("file_size", col("_metadata.file_size"))

# COMMAND ----------

df_bronze.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.01_bronze.sb_dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Processing

# COMMAND ----------

df_silver = spark.read.table("fmcg.01_bronze.sb_dim_customers")

# COMMAND ----------

df_silver = df_silver.dropDuplicates()
df_silver = df_silver.withColumn('customer_id', col('customer_id').cast('string'))
df_silver = df_silver.withColumn('customer_name', initcap(trim(col('customer_name'))))

# COMMAND ----------

df_silver = df_silver.withColumn("city",when(col("city").startswith("B"), "Bengaluru")
                                .when(col("city").startswith("N"), "New Delhi")
                                .when(col("city").startswith("H"), "Hyderabad")
                                .otherwise(None)
)

# COMMAND ----------

df_silver = df_silver.withColumn('city', when(col('customer_id') == '789403', 'New Delhi')
                                 .when(col('customer_id') == '789420', 'Bengaluru')
                                 .when(col('customer_id') == '789521', 'Hyderabad')
                                 .when(col('customer_id') == '789603', 'Hyderabad')
                                 .otherwise(col('city'))
                                 )

# COMMAND ----------

df_silver = df_silver.withColumnRenamed("customer_id", "customer_code")\
                     .withColumn('customer', concat(col('customer_name'), lit('-'), col('city')))\
                     .withColumn('market', lit('India'))\
                     .withColumn('platform', lit('Sports Bar'))\
                     .withColumn('channel', lit('Acquisition'))

# COMMAND ----------

df_silver.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.02_silver.sb_dim_customers") 

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Processing

# COMMAND ----------

sportsbar_customers = spark.read.table("fmcg.02_silver.sb_dim_customers")

# COMMAND ----------

sportsbar_customers = sportsbar_customers.select('customer_code', 'customer', 'market', 'platform', 'channel')

# COMMAND ----------

atlikon_customers = spark.read.table("fmcg.02_silver.at_dim_customers")

# COMMAND ----------

dim_customers = atlikon_customers.union(sportsbar_customers)

# COMMAND ----------

dim_customers.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.03_gold.dim_customers") 