# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

start_date = '2024-01-01'
end_date = '2025-12-01'

# COMMAND ----------

df = spark.sql(f"""
               SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 month)) AS month_start_date
               """)

# COMMAND ----------

df = df.withColumn('datekey', date_format(col('month_start_date'), 'yyyyMMdd').cast('int'))
df = df.withColumn('year', year(col('month_start_date')))
df = df.withColumn('month_name', date_format(col('month_start_date'), 'MMMM'))
df = df.withColumn('month_short_name', date_format(col('month_start_date'), 'MMM'))
df = df.withColumn('quarter', concat(lit('Q'), quarter(col('month_start_date'))))
df = df.withColumn('year_quarter', concat(col('year'), lit('-'), col('quarter')))

# COMMAND ----------

df.write.mode('overwrite').format('delta').saveAsTable('fmcg.03_gold.dim_calendar')