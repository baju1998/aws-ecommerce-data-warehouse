import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# Get arguments passed to the Glue job
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET"]
)

BUCKET_NAME = args["S3_BUCKET"]

# Create Spark session
spark = SparkSession.builder.appName(
    "EcommerceDataTransformation"
).getOrCreate()


# Define S3 paths
RAW_PATH = f"s3://{BUCKET_NAME}/raw/olist/"
PROCESSED_PATH = f"s3://{BUCKET_NAME}/processed/"


# Load all source datasets
customers = spark.read.csv(
    RAW_PATH + "olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

orders = spark.read.csv(
    RAW_PATH + "olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

order_items = spark.read.csv(
    RAW_PATH + "olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

products = spark.read.csv(
    RAW_PATH + "olist_products_dataset.csv",
    header=True,
    inferSchema=True
)

payments = spark.read.csv(
    RAW_PATH + "olist_order_payments_dataset.csv",
    header=True,
    inferSchema=True
)

print("All five datasets loaded successfully!")


# Display dataset schema
orders.printSchema()

# Display first five rows
orders.show(5, truncate=False)

timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column in timestamp_columns:
    orders = orders.withColumn(
        column,
        F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss")
    )

money_type = DecimalType(12, 2)

order_items = (
    order_items
    .withColumn(
        "price",
        F.col("price").cast(money_type)
    )
    .withColumn(
        "freight_value",
        F.col("freight_value").cast(money_type)
    )
)

payments = payments.withColumn(
    "payment_value",
    F.col("payment_value").cast(money_type)
)

# Detect duplicate order IDs
duplicate_orders = (
    orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_orders.count()

if duplicate_count > 0:
    raise ValueError(
        f"Found {duplicate_count} duplicated order IDs"
    )

print("Orders uniqueness check passed!")

# Load all source datasets
customers = spark.read.csv(
    RAW_PATH + "olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

orders = spark.read.csv(
    RAW_PATH + "olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

order_items = spark.read.csv(
    RAW_PATH + "olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

products = spark.read.csv(
    RAW_PATH + "olist_products_dataset.csv",
    header=True,
    inferSchema=True
)

payments = spark.read.csv(
    RAW_PATH + "olist_order_payments_dataset.csv",
    header=True,
    inferSchema=True
)

print("All five datasets loaded successfully!")

timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column in timestamp_columns:
    orders = orders.withColumn(
        column,
        F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss")
    )


money_type = DecimalType(12, 2)

order_items = (
    order_items
    .withColumn(
        "price",
        F.col("price").cast(money_type)
    )
    .withColumn(
        "freight_value",
        F.col("freight_value").cast(money_type)
    )
)

payments = payments.withColumn(
    "payment_value",
    F.col("payment_value").cast(money_type)
)

# Detect duplicate order IDs
duplicate_orders = (
    orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_orders.count()

if duplicate_count > 0:
    raise ValueError(
        f"Found {duplicate_count} duplicated order IDs"
    )

print("Orders uniqueness check passed!")

dim_customer = customers.select(
    F.col("customer_id").alias("customer_key"),
    F.col("customer_unique_id"),
    F.col("customer_city"),
    F.col("customer_state")
)

print("Customer dimension prepared!")

dim_customer.printSchema()

print("\nOrders schema:")
orders.printSchema()

print("\nOrder items schema:")
order_items.printSchema()

print("\nCustomer dimension:")
dim_customer.show(5, truncate=False)

sellers = spark.read.csv(
    RAW_PATH + "olist_sellers_dataset.csv",
    header=True,
    inferSchema=True
)

print("Sellers dataset loaded successfully!")
sellers.printSchema()

dim_product = products.select(
    F.col("product_id").alias("product_key"),
    F.col("product_category_name"),
    F.col("product_weight_g").cast("int")
)

print("Product dimension prepared!")

dim_product.printSchema()
dim_product.show(5, truncate=False)


dim_seller = sellers.select(
    F.col("seller_id").alias("seller_key"),
    F.col("seller_city"),
    F.col("seller_state")
)

print("Seller dimension prepared!")

dim_seller.printSchema()
dim_seller.show(5, truncate=False)


dim_date = (
    orders
    .select(
        F.to_date("order_purchase_timestamp").alias("full_date")
    )
    .filter(F.col("full_date").isNotNull())
    .distinct()
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "year",
        F.year("full_date")
    )
    .withColumn(
        "month",
        F.month("full_date")
    )
    .withColumn(
        "day",
        F.dayofmonth("full_date")
    )
    .withColumn(
        "quarter",
        F.quarter("full_date")
    )
    .select(
        "date_key",
        "full_date",
        "year",
        "month",
        "day",
        "quarter"
    )
)


def check_unique_key(df, key_column, table_name):

    duplicates = (
        df.groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_keys = df.filter(
        F.col(key_column).isNull()
    ).count()

    if duplicates > 0 or null_keys > 0:
        raise ValueError(
            f"{table_name}: "
            f"{duplicates} duplicate keys, "
            f"{null_keys} null keys"
        )

    print(f"{table_name}: Key validation passed!")

check_unique_key(
    dim_customer, "customer_key", "dim_customer"
)

check_unique_key(
    dim_product, "product_key", "dim_product"
)

check_unique_key(
    dim_seller, "seller_key", "dim_seller"
)

check_unique_key(
    dim_date, "date_key", "dim_date"
)

sales_joined = order_items.join(
    orders.select(
        "order_id",
        "customer_id",
        "order_purchase_timestamp"
    ),
    on="order_id",
    how="left"
)

missing_orders = sales_joined.filter(
    F.col("customer_id").isNull()
).count()

if missing_orders > 0:
    raise ValueError(
        f"Found {missing_orders} order items "
        "without a matching order"
    )

print("Order relationship validation passed!")

fact_sales = (
    sales_joined
    .select(
        F.col("order_id"),
        F.col("order_item_id"),
        F.col("customer_id").alias("customer_key"),
        F.col("product_id").alias("product_key"),
        F.col("seller_id").alias("seller_key"),
        F.date_format(
            F.col("order_purchase_timestamp"),
            "yyyyMMdd"
        ).cast("int").alias("date_key"),
        F.col("price").alias("item_price"),
        F.col("freight_value"),
        (
            F.col("price") + F.col("freight_value")
        ).alias("total_item_amount")
    )
)

source_count = order_items.count()
fact_count = fact_sales.count()

print(f"Source order items: {source_count}")
print(f"Fact sales rows: {fact_count}")

if source_count != fact_count:
    raise ValueError(
        "Fact table row count does not match "
        "source order items!"
    )

print("Fact table row count validation passed!")

duplicate_items = (
    fact_sales
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_rows = fact_sales.filter(
    F.col("order_id").isNull()
    | F.col("order_item_id").isNull()
    | F.col("customer_key").isNull()
    | F.col("product_key").isNull()
    | F.col("seller_key").isNull()
    | F.col("date_key").isNull()
    | F.col("item_price").isNull()
    | F.col("freight_value").isNull()
    | (F.col("item_price") < 0)
    | (F.col("freight_value") < 0)
).count()

if duplicate_items > 0 or invalid_rows > 0:
    raise ValueError(
        f"Fact validation failed: "
        f"{duplicate_items} duplicate item keys, "
        f"{invalid_rows} invalid rows"
    )

print("Fact sales validation passed!")

print("\nFact sales schema:")
fact_sales.printSchema()

print("\nSample fact sales records:")
fact_sales.show(5, truncate=False)





def write_parquet(df, table_name):

    output_path = f"{PROCESSED_PATH}{table_name}/"

    print(f"Writing {table_name} to {output_path}")

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )

    print(f"{table_name} written successfully!")



write_parquet(dim_customer, "dim_customer")
write_parquet(dim_product, "dim_product")
write_parquet(dim_seller, "dim_seller")
write_parquet(dim_date, "dim_date")
write_parquet(fact_sales, "fact_sales")

print("\nAll transformed datasets written successfully!")



print("\nVerifying written Parquet datasets...")

for table_name in [
    "dim_customer",
    "dim_product",
    "dim_seller",
    "dim_date",
    "fact_sales"
]:
    path = f"{PROCESSED_PATH}{table_name}/"

    verified_df = spark.read.parquet(path)

    print(
        f"{table_name}: {verified_df.count()} rows"
    )