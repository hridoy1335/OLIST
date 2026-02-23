# Databricks notebook source
df = spark.read.table('olist_cata.landing_data.customers')
df = df.d
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------


