# Databricks notebook source
# MAGIC %run "./Config"

# COMMAND ----------

# MAGIC %run "./Utils"

# COMMAND ----------

# Orders
orders_raw = spark.read.option("multiline", "true").schema(orders_schema).json(orders_path)
orders_raw_with_changed_col = normalize_column_names(orders_raw)
orders_bronze = (orders_raw_with_changed_col
    .withColumn("order_date", parse_dmY(F.col("order_date")))
    .withColumn("ship_date", parse_dmY(F.col("ship_date")))
    #.filter("order_id IS NOT NULL AND customer_id IS NOT NULL AND product_id IS NOT NULL")
)

# COMMAND ----------

write_to_table(orders_bronze, "raw_orders")

# COMMAND ----------

dbutils.fs.cp(customers_path, "file:/tmp/Customer.xlsx")

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# Customers
import pandas as pd
# read excel using pandas
pdf = pd.read_excel("file:/tmp/Customer.xlsx")

df = spark.createDataFrame(pdf)

customers_bronze = normalize_column_names(df)

# COMMAND ----------

write_to_table(customers_bronze, "raw_customers")

# COMMAND ----------

# Products
products = spark.read.option("header", True).csv(products_path)
products_bronze = normalize_column_names(products)

# COMMAND ----------

write_to_table(products_bronze, "raw_products")

# COMMAND ----------

