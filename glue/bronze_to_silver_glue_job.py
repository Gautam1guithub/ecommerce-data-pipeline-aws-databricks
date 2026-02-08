"""
AWS Glue PySpark Job
Bronze to Silver Transformation for E-commerce Data Pipeline
"""

import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# -------------------------------------------------
# Initialize Spark & Glue Context
# -------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -------------------------------------------------
# Define Explicit Schema (Prevents Schema Drift)
# -------------------------------------------------
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("discount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("city", StringType(), True)
])

# -------------------------------------------------
# Read Bronze Layer Data (CSV from S3)
# -------------------------------------------------
bronze_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("s3://glue-raw-bucket93/ecommerce/raw/ecommerce_data_5000.csv")

# -------------------------------------------------
# Data Cleaning & Transformation
# -------------------------------------------------
# Remove invalid records
silver_df = bronze_df.dropna(subset=["order_id", "total_amount"])

# Convert order_date to timestamp
silver_df = silver_df.withColumn(
    "order_date",
    to_timestamp(col("order_date"), "yyyy-MM-dd")
)

# -------------------------------------------------
# Write to Silver Layer (Parquet Format)
# -------------------------------------------------
silver_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3://glue-trans-bucket93/silver-layer-folder/")

print("✅ AWS Glue job completed: Bronze to Silver transformation successful")
