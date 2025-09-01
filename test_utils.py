# Databricks notebook source
pip install pytest

# COMMAND ----------

import pytest
from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ../Utils

# COMMAND ----------

@pytest.fixture(scope="session")
def spark_session():
    """Return a Spark session for tests (uses global spark in Databricks)."""
    try:
        return spark  # in Databricks
    except NameError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.appName("pytest-utils").getOrCreate()

# COMMAND ----------

@pytest.fixture
def sample_orders(spark_session):
    """Sample Orders DataFrame."""
    return spark_session.createDataFrame([
        ("O1","C1","P1","2017-02-01", 10.234),
        ("O2","C1","P2","2018-05-10", 20.235),
        ("O3","C2","P1","2019-01-15",-5.335),
    ], ["order_id","customer_id","product_id","order_date","profit"]) \
    .withColumn("order_date", F.to_date("order_date"))

# COMMAND ----------

@pytest.fixture
def sample_customers(spark_session):
    """Sample Customers DataFrame."""
    return spark_session.createDataFrame([
        ("C1","Alice","US"),
        ("C2","Bob","UK")
    ], ["customer_id","customer_name","country"])

# COMMAND ----------

@pytest.fixture
def sample_products(spark_session):
    """Sample Products DataFrame."""
    return spark_session.createDataFrame([
        ("P1","Furniture","Chairs"),
        ("P2","Technology","Phones"),
    ], ["product_id","category","sub_category"])

# COMMAND ----------

# MAGIC %md
# MAGIC Test Cases

# COMMAND ----------

@pytest.mark.unit
def test_parse_dmY_handles_multiple_formats(spark_session):
    df = spark_session.createDataFrame([("1/2/2017",), ("01/02/2017",)], ["raw"])
    parsed = df.select(parse_dmY(F.col("raw")).alias("d")).collect()
    result = [r.d.strftime("%Y-%m-%d") for r in parsed]
    assert result == ["2017-02-01", "2017-02-01"]

# COMMAND ----------

@pytest.mark.unit
def test_normalize_column_names(spark_session):
    df = spark_session.createDataFrame([(1, "Alice")], ["Order ID","Customer-Name"])
    normalized = normalize_column_names(df)
    assert set(normalized.columns) == {"order_id","customer_name"}

# COMMAND ----------

@pytest.mark.integration
def test_write_and_read_table(spark_session, tmp_path):
    df = spark_session.createDataFrame([(1,"Alice")], ["id","name"])
    table_name = "tmp_test_table"

    write_to_table(df, table_name, mode="overwrite")
    df_read = read_table(table_name)

    assert isinstance(df_read, DataFrame)
    rows = df_read.collect()
    assert rows[0]["name"] == "Alice"

# COMMAND ----------

@pytest.mark.unit
def test_enrich_orders(sample_orders, sample_customers, sample_products):
    enriched = enrich_orders(sample_orders, sample_customers, sample_products)
    cols = set(enriched.columns)
    # Ensure enrichment adds expected fields
    assert {"customer_name","country","category","sub_category","profit_rounded","order_year"}.issubset(cols)
    # Verify profit rounding works
    profits = [r.profit_rounded for r in enriched.collect()]
    assert all(isinstance(p, float) for p in profits)

# COMMAND ----------

@pytest.mark.unit
def test_aggregate_df(sample_orders):
    df = sample_orders.withColumn("profit_rounded", F.round("profit", 2)) \
                      .withColumn("order_year", F.year("order_date"))

    agg = aggregate_df(df, ["order_year"], "profit_rounded", 2)
    results = {r["order_year"]: r["total_profit"] for r in agg.collect()}

    # Check that sums are rounded correctly
    assert 2017 in results and round(results[2017],2) == 10.23
    assert 2018 in results and round(results[2018],2) == 20.24
    assert 2019 in results and round(results[2019],2) == -5.34

# COMMAND ----------

