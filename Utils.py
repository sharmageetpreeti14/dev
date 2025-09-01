# Databricks notebook source
# MAGIC %md
# MAGIC Helpers Functions

# COMMAND ----------

def parse_dmY(col):
    d1 = F.to_date(col, "d/M/yyyy")
    d2 = F.to_date(col, "dd/MM/yyyy")
    return F.coalesce(d1, d2)

# COMMAND ----------

def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardize column names:
    - Lowercase
    - Replace spaces with underscores

    :param df: Spark DataFrame
    :return: Spark DataFrame with renamed columns
    """
    for col in df.columns:
        new_col = col.lower().replace(" ", "_").replace("-","_")
        df = df.withColumnRenamed(col, new_col)
    return df

# COMMAND ----------

def write_to_table(df, table_name: str, mode: str = "overwrite"):
    """
    Write a Spark DataFrame to a managed table in the metastore.

    :param df: Spark DataFrame
    :param table_name: Target table name (string)
    :param mode: Write mode - 'overwrite', 'append', 'ignore', 'errorifexists'
    """
    try:
        df.write.mode(mode).saveAsTable(table_name)
        print(f"Data written to table `{table_name}` with mode `{mode}`.")
    except Exception as e:
        print(f"Failed to write to table {table_name}: {e}")

# COMMAND ----------

def read_table(table_name: str) -> DataFrame:
    """
    Reads a Spark table and returns a DataFrame.
    
    :param table_name: Name of the table in the metastore
    :return: Spark DataFrame
    """
    try:
        df = spark.table(table_name)
        print(f"Successfully read table `{table_name}`.")
        return df
    except Exception as e:
        print(f"Failed to read table {table_name}: {e}")
        return None

# COMMAND ----------

def enrich_orders(orders: DataFrame, 
                  customers_silver: DataFrame, 
                  products_silver: DataFrame) -> DataFrame:
    """
    Enriches orders DataFrame by joining with customers and products,
    and adding calculated columns.

    :param orders: Orders DataFrame
    :param customers_silver: Customers DataFrame
    :param products_silver: Products DataFrame
    :return: Enriched Orders DataFrame
    """
    
    customers_sel = customers_silver.select("customer_id", "customer_name", "country")
    products_sel  = products_silver.select("product_id", "category", "sub_category")
    
    orders_enriched = (
        orders
        .join(customers_sel, "customer_id", "left")
        .join(products_sel, "product_id", "left")
        .withColumn("profit_rounded", F.round("profit", 2))
        .withColumn("order_year", F.year("order_date"))
    )
    
    return orders_enriched

# COMMAND ----------

def aggregate_df(df: DataFrame, group_by_cols: list, agg_col: str, round_to: int = 2) -> DataFrame:
    """
    Aggregates a DataFrame by grouping on given columns and summing a target column with rounding.

    :param df: Input Spark DataFrame
    :param group_by_cols: List of column names to group by
    :param agg_col: Column to aggregate (sum)
    :param round_to: Decimal precision for rounding
    :return: Aggregated DataFrame
    """
    agg_df = (
        df.groupBy(*group_by_cols)
          .agg(F.round(F.sum(F.col(agg_col)), round_to).alias("total_profit"))
    )
    return agg_df