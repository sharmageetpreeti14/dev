# Databricks notebook source
#import statement
from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame

# COMMAND ----------

# ---------------- Input File Path ----------------
orders_path = "dbfs:/FileStore/tables/Orders.json"
customers_path = "dbfs:/FileStore/tables/Customer.xlsx"
products_path = "dbfs:/FileStore/tables/Products__1_.csv"

# COMMAND ----------

# ---------------- Schemas ----------------
orders_schema = T.StructType([
    T.StructField("Row ID", T.IntegerType()),
    T.StructField("Order ID", T.StringType()),
    T.StructField("Order Date", T.StringType()),
    T.StructField("Ship Date", T.StringType()),
    T.StructField("Ship Mode", T.StringType()),
    T.StructField("Customer ID", T.StringType()),
    T.StructField("Product ID", T.StringType()),
    T.StructField("Quantity", T.IntegerType()),
    T.StructField("Price", T.DoubleType()),
    T.StructField("Discount", T.DoubleType()),
    T.StructField("Profit", T.DoubleType())
])