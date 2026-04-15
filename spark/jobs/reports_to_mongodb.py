import os
from pyspark.sql import SparkSession, functions as F, Window

POSTGRES_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/bigdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "bigdata")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "bigdata")
POSTGRES_DRIVER = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "reports")

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "star")


def get_spark():
    jars = "/opt/spark-apps/jars/postgresql-42.7.1.jar,/opt/spark-apps/jars/mongo-spark-connector_2.12-10.4.0.jar,/opt/spark-apps/jars/mongodb-driver-sync-4.11.1.jar,/opt/spark-apps/jars/mongodb-driver-core-4.11.1.jar,/opt/spark-apps/jars/bson-4.11.1.jar"
    return (
        SparkSession.builder.appName("reports_to_mongodb")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", jars)
        .config("spark.jars.ivy", "/tmp/.ivy")
        .config("spark.executor.extraClassPath", jars)
        .config("spark.driver.extraClassPath", jars)
        .config("spark.mongodb.connection.uri", MONGODB_URI)
        .config("spark.mongodb.connection.database", MONGODB_DATABASE)
        .getOrCreate()
    )


def read_pg_table(spark, table):
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", f"{SOURCE_SCHEMA}.{table}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .load()
    )


def write_mongodb(df, collection):
    df.write.format("mongodb") \
        .mode("overwrite") \
        .option("collection", collection) \
        .option("database", MONGODB_DATABASE) \
        .option("connection.uri", MONGODB_URI) \
        .save()


def main():
    spark = get_spark()

    print("Reading data from PostgreSQL...")
    fact = read_pg_table(spark, "fact_sales")
    dim_product = read_pg_table(spark, "dim_product")
    dim_customer = read_pg_table(spark, "dim_customer")
    dim_store = read_pg_table(spark, "dim_store")
    dim_supplier = read_pg_table(spark, "dim_supplier")
    dim_date = read_pg_table(spark, "dim_date")

    fact_product = fact.join(dim_product, on="product_id", how="left")

    product_base = (
        fact_product.groupBy("product_id", "product_name", "product_category")
        .agg(
            F.sum("sale_quantity").alias("total_sales_qty"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("product_rating").alias("avg_rating"),
            F.sum("product_reviews").alias("total_reviews"),
        )
    )

    category_revenue = (
        product_base.groupBy("product_category")
        .agg(F.sum("total_revenue").alias("category_revenue"))
    )

    product_rank_window = Window.orderBy(F.col("total_sales_qty").desc_nulls_last())
    product_report = (
        product_base.join(category_revenue, on="product_category", how="left")
        .withColumn("product_rank", F.row_number().over(product_rank_window))
    )

    write_mongodb(product_report, "report_product_sales")
    print(f"report_product_sales written")

    customer_base = (
        fact.join(dim_customer, on="customer_id", how="left")
        .groupBy(
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
        )
        .agg(
            F.sum("sale_total_price").alias("total_spent"),
            F.avg("sale_total_price").alias("avg_check"),
        )
    )

    country_counts = (
        dim_customer.groupBy("customer_country")
        .agg(F.countDistinct("customer_id").alias("country_customer_count"))
    )

    customer_rank_window = Window.orderBy(F.col("total_spent").desc_nulls_last())
    customer_report = (
        customer_base.join(country_counts, on="customer_country", how="left")
        .withColumn("customer_rank", F.row_number().over(customer_rank_window))
    )

    write_mongodb(customer_report, "report_customer_sales")
    print(f"report_customer_sales written")

    fact_date = fact.join(dim_date, fact.sale_date_id == dim_date.date_id, how="left")

    time_report = (
        fact_date.groupBy("year", "month")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.countDistinct("sale_id").alias("orders"),
            F.avg("sale_total_price").alias("avg_order_value"),
        )
        .withColumn("year_total_revenue", F.sum("total_revenue").over(Window.partitionBy("year")))
        .orderBy("year", "month")
    )

    write_mongodb(time_report, "report_time_sales")
    print(f"report_time_sales written")

    store_base = (
        fact.join(dim_store, on="store_id", how="left")
        .groupBy(
            "store_id",
            "store_name",
            "store_city",
            "store_country",
        )
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("sale_total_price").alias("avg_check"),
        )
    )

    store_rank_window = Window.orderBy(F.col("total_revenue").desc_nulls_last())
    city_sales = (
        store_base.groupBy("store_city")
        .agg(F.sum("total_revenue").alias("city_total_revenue"))
    )
    country_sales = (
        store_base.groupBy("store_country")
        .agg(F.sum("total_revenue").alias("country_total_revenue"))
    )

    store_report = (
        store_base.join(city_sales, on="store_city", how="left")
        .join(country_sales, on="store_country", how="left")
        .withColumn("store_rank", F.row_number().over(store_rank_window))
    )

    write_mongodb(store_report, "report_store_sales")
    print(f"report_store_sales written")

    supplier_base = (
        fact_product.join(dim_supplier, on="supplier_id", how="left")
        .groupBy("supplier_id", "supplier_name", "supplier_country")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("product_price").alias("avg_product_price"),
        )
    )

    supplier_rank_window = Window.orderBy(F.col("total_revenue").desc_nulls_last())
    supplier_country_sales = (
        supplier_base.groupBy("supplier_country")
        .agg(F.sum("total_revenue").alias("country_total_revenue"))
    )

    supplier_report = (
        supplier_base.join(supplier_country_sales, on="supplier_country", how="left")
        .withColumn("supplier_rank", F.row_number().over(supplier_rank_window))
    )

    write_mongodb(supplier_report, "report_supplier_sales")
    print(f"report_supplier_sales written")

    product_sales = (
        fact_product.groupBy("product_id", "product_name", "product_rating", "product_reviews")
        .agg(
            F.sum("sale_quantity").alias("total_sales_qty"),
            F.sum("sale_total_price").alias("total_revenue"),
        )
    )

    corr_value = product_sales.select(F.corr("product_rating", "total_sales_qty").alias("corr")).collect()[0]["corr"]

    rating_desc_window = Window.orderBy(F.col("product_rating").desc_nulls_last())
    rating_asc_window = Window.orderBy(F.col("product_rating").asc_nulls_last())

    quality_report = (
        product_sales
        .withColumn("rating_rank_desc", F.row_number().over(rating_desc_window))
        .withColumn("rating_rank_asc", F.row_number().over(rating_asc_window))
        .withColumn("rating_sales_corr", F.lit(corr_value))
    )

    write_mongodb(quality_report, "report_quality")
    print(f"report_quality written")

    spark.stop()
    print("All reports written successfully!")


if __name__ == "__main__":
    main()
