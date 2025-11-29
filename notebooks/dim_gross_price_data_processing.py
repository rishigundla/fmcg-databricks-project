# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Processing

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

df_bronze = spark.read.format("csv")\
                 .option("header", "true")\
                 .option("inferSchema", "true")\
                 .load("abfss://fmcg@dataenggadls001.dfs.core.windows.net/sportsbar/gross_price/")

# COMMAND ----------

df_bronze = df_bronze.withColumn("read_timestamp", current_timestamp())
df_bronze = df_bronze.withColumn("file_name", col("_metadata.file_name"))
df_bronze = df_bronze.withColumn("file_size", col("_metadata.file_size"))

# COMMAND ----------

df_bronze.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.01_bronze.sb_dim_gross_price")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Processing

# COMMAND ----------

df_silver = spark.read.table("fmcg.01_bronze.sb_dim_gross_price")

# COMMAND ----------

df_silver = df_silver.dropDuplicates()

# COMMAND ----------

date_formats = ["yyyy/MM/dd","dd/MM/yyyy","yyyy-MM-dd", "dd-MM-yyyy"]

df_silver = df_silver.withColumn("month",coalesce(
                                                try_to_date(col("month"), "yyyy/MM/dd"),
                                                try_to_date(col("month"),"dd/MM/yyyy"),
                                                try_to_date(col("month"), "yyyy-MM-dd"),
                                                try_to_date(col("month"), "dd-MM-yyyy")
                                                )
)

# COMMAND ----------

df_silver = df_silver.withColumn("gross_price", when(col("gross_price").rlike(r"^-?\d+(\.\d+)?$"), col("gross_price").cast("double")).otherwise(0))
df_silver = df_silver.withColumn("gross_price", when(col("gross_price") < 0, col("gross_price") * -1).otherwise(col("gross_price")))

# COMMAND ----------

df_silver = df_silver.withColumn("is_zero", when(col("gross_price") == 0, 1).otherwise(0))
df_silver = df_silver.withColumn("year", year(col("month")))

# COMMAND ----------

df_silver = df_silver.withColumn("rnk", row_number().over(Window.partitionBy("product_id", "year").orderBy(col("is_zero"), col("month").desc())))
df_silver = df_silver.filter(col("rnk") == 1)
df_silver = df_silver.withColumnRenamed("gross_price", "price_inr")

# COMMAND ----------

df_silver = df_silver.withColumn("product_id", col("product_id").cast('string'))\
                     .withColumnRenamed("product_id", "product_code")

# COMMAND ----------

df_silver.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.02_silver.sb_dim_gross_price") 

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Processing

# COMMAND ----------

sportsbar_gross_price = spark.read.table("fmcg.02_silver.sb_dim_gross_price")

# COMMAND ----------

sportsbar_gross_price = sportsbar_gross_price.select('product_code', 'price_inr', 'year')

# COMMAND ----------

atlikon_gross_price = spark.read.table("fmcg.02_silver.at_dim_gross_price")

# COMMAND ----------

dim_gross_price = atlikon_gross_price.union(sportsbar_gross_price)

# COMMAND ----------

dim_gross_price.write.format("delta") \
         .mode("overwrite") \
         .option("delta.enableChangeDataFeed", "true") \
         .saveAsTable("fmcg.03_gold.dim_gross_price") 