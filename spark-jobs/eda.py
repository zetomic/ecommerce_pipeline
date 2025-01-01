from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, avg, desc, count as _count, to_timestamp, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("RealTimeEDA") \
        .getOrCreate()

    logger.info("Spark session started.")

    kafka_bootstrap_servers = "kafka:9092"
    input_topic = "ecommerce"
    output_topic = "eda_results"

    # Define the schema based on your dataset
    json_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("brand_name", StringType(), True),
        StructField("manufacturer_name", StringType(), True),
        StructField("l0_category", StringType(), True),
        StructField("l1_category", StringType(), True),
        StructField("l2_category", StringType(), True),
        StructField("l0_category_id", IntegerType(), True),
        StructField("l1_category_id", IntegerType(), True),
        StructField("l2_category_id", IntegerType(), True),
        StructField("date_", StringType(), True),
        StructField("city_name", StringType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("cart_id", IntegerType(), True),
        StructField("dim_customer_key", IntegerType(), True),
        StructField("procured_quantity", IntegerType(), True),
        StructField("unit_selling_price", DoubleType(), True),
        StructField("total_discount_amount", DoubleType(), True),
        StructField("total_weighted_landing_price", DoubleType(), True),
    ])

    # Read stream from Kafka
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    logger.info("Data read from Kafka.")

    # Parse JSON
    df_parsed = (
        df_kafka
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), json_schema).alias("data"))
        .select("data.*")
    )

    logger.info("JSON parsed successfully.")

    # Convert 'date_' to timestamp
    df_parsed = df_parsed.withColumn("timestamp", to_timestamp(col("date_"), "yyyy-MM-dd HH:mm:ss"))

    # Define EDA Aggregations

    # 1. Total Sales by Category
    sales_by_category = df_parsed.groupBy("l1_category").agg(
        _sum("unit_selling_price").alias("total_sales")
    ).withColumn("metric", lit("sales_by_category"))

    # 2. Quantity Procured by City
    quantity_by_city = df_parsed.groupBy("city_name").agg(
        _sum("procured_quantity").alias("total_procured_quantity")
    ).withColumn("metric", lit("quantity_by_city"))

    # 3. Top 10 Brands
    top_brands = df_parsed.groupBy("brand_name").agg(
        _count("brand_name").alias("brand_count")
    ).orderBy(desc("brand_count")).limit(10).withColumn("metric", lit("top_brands"))

    # 4. Sales Trend Over Time
    sales_trend = df_parsed.groupBy(
        window(col("timestamp"), "10 minutes"),
        "l1_category"
    ).agg(
        _sum("unit_selling_price").alias("sales")
    ).withColumn("metric", lit("sales_trend"))

    # Additional EDA Aggregations

    # 5. Average Unit Selling Price by Category
    avg_price_by_category = df_parsed.groupBy("l1_category").agg(
        avg("unit_selling_price").alias("avg_unit_selling_price")
    ).withColumn("metric", lit("avg_price_by_category"))

    # 6. Total Discount Amount by Brand
    discount_by_brand = df_parsed.groupBy("brand_name").agg(
        _sum("total_discount_amount").alias("total_discount_amount")
    ).withColumn("metric", lit("discount_by_brand"))

    # 7. Number of Orders per City
    orders_by_city = df_parsed.groupBy("city_name").agg(
        _count("order_id").alias("total_orders")
    ).withColumn("metric", lit("orders_by_city"))

    # 8. Top 10 Products by Quantity Sold
    top_products = df_parsed.groupBy("product_name").agg(
        _sum("procured_quantity").alias("total_quantity_sold")
    ).orderBy(desc("total_quantity_sold")).limit(10).withColumn("metric", lit("top_products"))

    # 9. Average Order Value (AOV) by Customer
    aov_by_customer = df_parsed.groupBy("dim_customer_key").agg(
        _sum("unit_selling_price").alias("total_spent"),
        _count("order_id").alias("order_count")
    ).withColumn("average_order_value", col("total_spent") / col("order_count")) \
     .withColumn("metric", lit("aov_by_customer"))

    logger.info("Aggregations defined.")

    # Function to write each aggregation to Kafka using Spark's native Kafka integration
    def write_to_kafka(df, epoch_id, metric_name):
        if df.isEmpty():
            logger.info(f"No data to send for metric '{metric_name}' at epoch {epoch_id}.")
            return
        # Convert DataFrame to JSON with metric label
        json_df = df.select(to_json(struct("*")).alias("value"))
        
        # Write to Kafka
        json_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", output_topic) \
            .option("checkpointLocation", f"/tmp/{metric_name}_checkpoints") \
            .save()
        
        logger.info(f"Sent data for metric '{metric_name}' to Kafka at epoch {epoch_id}.")

    # Define write streams for each aggregation
    sales_by_category_query = sales_by_category.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_to_kafka(df, epoch_id, "sales_by_category")) \
        .start()

    quantity_by_city_query = quantity_by_city.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_to_kafka(df, epoch_id, "quantity_by_city")) \
        .start()

    top_brands_query = top_brands.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_to_kafka(df, epoch_id, "top_brands")) \
        .start()

    sales_trend_query = sales_trend.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_to_kafka(df, epoch_id, "sales_trend")) \
        .start()

    avg_price_by_category_query = avg_price_by_category.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_to_kafka(df, epoch_id, "avg_price_by_category")) \
        .start()

    discount_by_brand_query = discount_by_brand.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_to_kafka(df, epoch_id, "discount_by_brand")) \
        .start()

    orders_by_city_query = orders_by_city.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_to_kafka(df, epoch_id, "orders_by_city")) \
        .start()

    top_products_query = top_products.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_to_kafka(df, epoch_id, "top_products")) \
        .start()

    aov_by_customer_query = aov_by_customer.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch_id: write_to_kafka(df, epoch_id, "aov_by_customer")) \
        .start()

    logger.info("Write streams started.")

    # Await termination to keep the streaming job alive
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
