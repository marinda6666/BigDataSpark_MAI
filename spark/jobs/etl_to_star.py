import os
from pyspark.sql import SparkSession, functions as F, Window

POSTGRES_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/bigdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "bigdata")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "bigdata")
POSTGRES_DRIVER = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")

SOURCE_TABLE = os.getenv("SOURCE_TABLE", "mock_data")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "star")


def get_spark():
    return (
        SparkSession.builder.appName("etl_to_star")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_source(spark):
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", SOURCE_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .load()
    )


def write_table(df, table):
    (
        df.write.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", f"{TARGET_SCHEMA}.{table}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .mode("overwrite")
        .save()
    )


def main():
    spark = get_spark()

    df = read_source(spark)

    df = (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("customer_age", F.col("customer_age").cast("int"))
        .withColumn("product_price", F.col("product_price").cast("double"))
        .withColumn("product_quantity", F.col("product_quantity").cast("int"))
        .withColumn("sale_customer_id", F.col("sale_customer_id").cast("long"))
        .withColumn("sale_seller_id", F.col("sale_seller_id").cast("long"))
        .withColumn("sale_product_id", F.col("sale_product_id").cast("long"))
        .withColumn("sale_quantity", F.col("sale_quantity").cast("int"))
        .withColumn("sale_total_price", F.col("sale_total_price").cast("double"))
        .withColumn("product_weight", F.col("product_weight").cast("double"))
        .withColumn("product_rating", F.col("product_rating").cast("double"))
        .withColumn("product_reviews", F.col("product_reviews").cast("int"))
        .withColumn("sale_date", F.to_date("sale_date", "M/d/yyyy"))
        .withColumn("product_release_date", F.to_date("product_release_date", "M/d/yyyy"))
        .withColumn("product_expiry_date", F.to_date("product_expiry_date", "M/d/yyyy"))
    )

    dim_customer = (
        df.select(
            F.col("sale_customer_id").alias("customer_id"),
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
            "pet_category",
        )
        .dropDuplicates(["customer_id"])
    )

    dim_seller = (
        df.select(
            F.col("sale_seller_id").alias("seller_id"),
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
        )
        .dropDuplicates(["seller_id"])
    )

    supplier_cols = [
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ]
    supplier_window = Window.orderBy(*[F.col(c).asc_nulls_last() for c in supplier_cols])
    dim_supplier = (
        df.select(*supplier_cols)
        .dropDuplicates()
        .withColumn("supplier_id", F.row_number().over(supplier_window))
    )

    dim_product = (
        df.select(
            F.col("sale_product_id").alias("product_id"),
            "product_name",
            "product_category",
            "product_price",
            "product_quantity",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
            *supplier_cols,
        )
        .dropDuplicates(["product_id"])
        .join(dim_supplier, on=supplier_cols, how="left")
        .drop(*supplier_cols)
    )

    store_cols = [
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    ]
    store_window = Window.orderBy(*[F.col(c).asc_nulls_last() for c in store_cols])
    dim_store = (
        df.select(*store_cols)
        .dropDuplicates()
        .withColumn("store_id", F.row_number().over(store_window))
    )

    dim_date = (
        df.select("sale_date")
        .dropna()
        .dropDuplicates()
        .withColumn("date_id", F.date_format("sale_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .withColumn("day", F.dayofmonth("sale_date"))
        .withColumn("quarter", F.quarter("sale_date"))
    )

    fact_sales = (
        df.join(dim_store, on=store_cols, how="left")
        .join(dim_date, on="sale_date", how="left")
        .select(
            F.col("id").alias("sale_id"),
            F.col("date_id").alias("sale_date_id"),
            F.col("sale_customer_id").alias("customer_id"),
            F.col("sale_seller_id").alias("seller_id"),
            F.col("sale_product_id").alias("product_id"),
            F.col("store_id"),
            F.col("sale_quantity"),
            F.col("sale_total_price"),
        )
    )

    write_table(dim_customer, "dim_customer")
    write_table(dim_seller, "dim_seller")
    write_table(dim_supplier, "dim_supplier")
    write_table(dim_product, "dim_product")
    write_table(dim_store, "dim_store")
    write_table(dim_date, "dim_date")
    write_table(fact_sales, "fact_sales")

    spark.stop()


if __name__ == "__main__":
    main()
