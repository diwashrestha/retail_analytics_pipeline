import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, DateType
)

# Start a Spark Session
spark = SparkSession.builder \
    .appName("EinKaufPark Bronze Layer") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Define the bronze schema
bronze_schema = StructType([
    # -- Transaction Identifiers --
    StructField("transaction_id", StringType(), True),
    StructField("batch_id", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("record_hash", StringType(), False),

    # -- Dates --
    StructField("order_date", DateType(), False),
    StructField("ship_date", DateType(), False),
    StructField("ingestion_date", DateType(), False),

    # -- Sales Channel --
    StructField("sales_channel", StringType(), False),

    # -- Store & Geography --
    StructField("store_id", StringType(), False),
    StructField("store_city", StringType(), False),
    StructField("store_district", StringType(), True),
    StructField("store_postal_code", StringType(), True),
    StructField("store_area", StringType(), True),
    StructField("store_region", StringType(), False),
    StructField("store_country_code", StringType(), False),
    StructField("store_country_name", StringType(), False),
    StructField("store_size_class", StringType(), False),

    # -- Customer Details --
    StructField("customer_id", StringType(), False),
    StructField("customer_age", IntegerType(), True),
    StructField("gender", StringType(), True),

    # -- Loyalty --
    StructField("loyalty_card_id", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("loyalty_points_earned", IntegerType(), True),
    StructField("coupon_applied", BooleanType(), True),
    StructField("coupon_code", StringType(), True),

    # -- Product Details --
    StructField("product_id", StringType(), False),
    StructField("product_category", StringType(), False),
    StructField("product_subcategory", StringType(), False),
    StructField("is_private_label", BooleanType(), False),
    StructField("brand", StringType(), False),

    # -- Pricing --
    StructField("quantity", IntegerType(), True),
    StructField("unit_price_eur", FloatType(), True),
    StructField("discount_pct", FloatType(), True),
    StructField("transaction_currency", StringType(), False),
    StructField("net_revenue_eur", FloatType(), True),

    # -- Payment & Order Status --
    StructField("payment_type", StringType(), True),
    StructField("order_status", StringType(), False),

    # -- Operational Metadata --
    StructField("pos_terminal_id", StringType(), True),
    StructField("cashier_id", StringType(), True),
    StructField("promo_week_id", StringType(), False),
    StructField("is_promo_period", BooleanType(), False),
    StructField("data_quality_flag", StringType(), False),
])

# -- Read the CSV into a bronze DataFrame
bronze_df = spark.read \
    .option("header", True) \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("nullValue", "") \
    .option("mode", "PERMISSIVE") \
    .schema(bronze_schema) \
    .csv("data/raw/einkaufpark_de_sales_raw.csv")

print(f"Count of rows in Bronze DataFrame: {bronze_df.count()}")

print(f"Bronze DataFrame Schema:\n")
bronze_df.printSchema()

bronze_path = os.path.join(os.getcwd(), "data", "bronze", "einkaufpark_de_sales_bronze.parquet")

bronze_df.write.mode("overwrite").parquet(bronze_path)
print(f"✅ Bronze parquet written to: {bronze_path}")