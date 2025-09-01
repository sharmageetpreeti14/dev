# Databricks notebook source
# MAGIC %run "./Config"

# COMMAND ----------

# MAGIC %run "./Utils"

# COMMAND ----------

orders_enriched = read_table("fact_orders_enriched")

agg = aggregate_df(orders_enriched,["order_year", "category", "sub_category", "customer_id", "customer_name"],"profit_rounded")

write_to_table(agg, "agg_profit_by_dims")


# COMMAND ----------

