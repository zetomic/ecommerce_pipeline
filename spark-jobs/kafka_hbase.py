from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import happybase
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def write_to_hbase(partition, table_name):
    """
    Writes records from a partition to the specified HBase table.
    """
    try:
        # Establish connection to HBase
        connection = happybase.Connection('hbase-master', port=9090)  # Replace with your HBase host and port
        connection.open()
        table = connection.table(table_name)

        for record in partition:
            record_dict = record.asDict()

            # Define row key using 'order_id' to ensure uniqueness
            order_id = record_dict.get('order_id')
            if not order_id:
                logger.warning(f"Record without 'order_id': {record_dict}")
                continue
            row_key = str(order_id)

            # Prepare HBase columns under respective column families
            columns = {
                'product_data:product_id': str(record_dict.get('product_id', '')),
                'product_data:product_name': record_dict.get('product_name', ''),
                'product_data:unit': record_dict.get('unit', ''),
                'product_data:product_type': record_dict.get('product_type', ''),
                'product_data:brand_name': record_dict.get('brand_name', ''),
                'product_data:manufacturer_name': record_dict.get('manufacturer_name', ''),
                'product_data:l0_category': record_dict.get('l0_category', ''),
                'product_data:l1_category': record_dict.get('l1_category', ''),
                'product_data:l2_category': record_dict.get('l2_category', ''),
                'product_data:l0_category_id': str(record_dict.get('l0_category_id', '')),
                'product_data:l1_category_id': str(record_dict.get('l1_category_id', '')),
                'product_data:l2_category_id': str(record_dict.get('l2_category_id', '')),

                'order_data:order_id': str(record_dict.get('order_id', '')),
                'order_data:cart_id': str(record_dict.get('cart_id', '')),

                'customer_data:dim_customer_key': str(record_dict.get('dim_customer_key', '')),

                'pricing:procured_quantity': str(record_dict.get('procured_quantity', '')),
                'pricing:unit_selling_price': str(record_dict.get('unit_selling_price', '')),
                'pricing:total_discount_amount': str(record_dict.get('total_discount_amount', '')),
                'pricing:total_weighted_landing_price': str(record_dict.get('total_weighted_landing_price', '')),

                'metadata:date_': record_dict.get('date_', ''),
                'metadata:city_name': record_dict.get('city_name', '')
            }

            # Write to HBase
            table.put(row_key.encode('utf-8'), columns)
            logger.info(f"Written to HBase table '{table_name}': Order ID {order_id}")

    except Exception as e:
        logger.error(f"Error writing to HBase table '{table_name}': {e}")

    finally:
        connection.close()

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("EcommerceDataToHBase") \
        .getOrCreate()

    logger.info("Spark session started.")

    kafka_bootstrap_servers = "kafka:9092"
    input_topic = "ecommerce"
    table_name = "ecommerce_data"

    # Define schema based on example data
    json_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("brand_name", StringType(), True),
        StructField("manufacturer_name", StringType(), True),
        StructField("l0_category", StringType(), True),
        StructField("l1_category", StringType(), True),
        StructField("l2_category", StringType(), True),
        StructField("l0_category_id", StringType(), True),
        StructField("l1_category_id", StringType(), True),
        StructField("l2_category_id", StringType(), True),
        StructField("date_", StringType(), True),
        StructField("city_name", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("cart_id", StringType(), True),
        StructField("dim_customer_key", StringType(), True),
        StructField("procured_quantity", StringType(), True),
        StructField("unit_selling_price", StringType(), True),
        StructField("total_discount_amount", StringType(), True),
        StructField("total_weighted_landing_price", StringType(), True),
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

    # Function to process each batch
    def foreach_batch_function(df, epoch_id):
        if df.rdd.isEmpty():
            logger.info(f"No data received in epoch {epoch_id}.")
            return

        # Use foreachPartition for better performance
        df.foreachPartition(lambda partition: write_to_hbase(partition, table_name))

    # Start streaming query
    query = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(foreach_batch_function) \
        .option("checkpointLocation", "/tmp/write_to_hbase_checkpoints") \
        .start()

    logger.info("Started streaming query to write to HBase.")

    # Await termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
