# Databricks notebook source
orders = spark.table("fact_orders_enriched")
orders.createOrReplaceTempView("vw_orders")

# COMMAND ----------

# Profit by Year
display(spark.sql("""
  SELECT order_year, ROUND(SUM(profit_rounded), 2) AS total_profit
  FROM vw_orders GROUP BY order_year ORDER BY order_year
"""))

# COMMAND ----------

# Profit by Year + Category
display(spark.sql("""
  SELECT order_year, category, ROUND(SUM(profit_rounded), 2) AS total_profit
  FROM vw_orders GROUP BY order_year, category ORDER BY order_year, category
"""))

# COMMAND ----------

# Profit by Customer
display(spark.sql("""
  SELECT customer_id, COALESCE(customer_name, customer_id) AS customer_name,
         ROUND(SUM(profit_rounded), 2) AS total_profit
  FROM vw_orders GROUP BY customer_id, customer_name ORDER BY total_profit DESC
"""))

# COMMAND ----------

# Profit by Customer + Year
display(spark.sql("""
  SELECT order_year, customer_id, COALESCE(customer_name, customer_id) AS customer_name,
         ROUND(SUM(profit_rounded), 2) AS total_profit
  FROM vw_orders GROUP BY order_year, customer_id, customer_name
  ORDER BY order_year, total_profit DESC
"""))

# COMMAND ----------

