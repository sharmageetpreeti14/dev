# Databricks notebook source
# MAGIC %run "./Config"

# COMMAND ----------

# MAGIC %run "./Utils"

# COMMAND ----------

customers_silver = read_table("raw_customers").dropDuplicates(["customer_id"])
write_to_table(customers_silver, "dim_customers")

# COMMAND ----------

from pyspark.sql.functions import col
display(customers_silver.filter(col("customer_id").isNull()))

# COMMAND ----------

products_silver = read_table("raw_products").dropDuplicates(["product_id"])
write_to_table(products_silver, "dim_products")

# COMMAND ----------

display(products_silver.filter(col("category").isNull()))

# COMMAND ----------

orders = read_table("raw_orders").filter("order_id IS NOT NULL AND customer_id IS NOT NULL AND product_id IS NOT NULL")
orders_enriched = enrich_orders(orders,customers_silver,products_silver)
write_to_table(orders_enriched, "fact_orders_enriched")

# COMMAND ----------

display(products_silver.filter(col("product_id")=="OFF-AR-10002280"))

# COMMAND ----------

display(orders_enriched.filter(col("category").isNull()))

# COMMAND ----------

