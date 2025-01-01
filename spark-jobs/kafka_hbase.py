from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import happybase
import json
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def write_to_hbase(record, table_name):
    """
    Writes a single record to the specified HBase table.
    """
    try:
        # Establish connection to HBase
        connection = happybase.Connection('hbase-master', port=9090)  # Replace with your HBase host and port
        connection.open()
        table = connection.table(table_name)

        # Prepare row key (you might need to choose an appropriate key based on your data)
        # For simplicity, using a combination of metric and a unique identifier
        # Here, assuming that each record has a unique field like 'product_name' or 'brand_name'
        if table_name == 'top_products':
            row_key = record.get('product_name', '')  # Ensure uniqueness
        elif table_name == 'top_brands':
            row_key = record.get('brand_name', '')
        elif table_name == 'sales_by_category' or table_name == 'avg_price_by_category':
            row_key = record.get('l1_category', '')
        elif table_name == 'quantity_by_city' or table_name == 'orders_by_city':
            row_key = record.get('city_name', '')
        elif table_name == 'sales_trend':
            row_key = f"{record.get('l1_category', '')}_{record.get('window', {}).get('start', '')}"
        elif table_name == 'discount_by_brand':
            row_key = record.get('brand_name', '')
        elif table_name == 'aov_by_customer':
            row_key = str(record.get('dim_customer_key', ''))
        else:
            row_key = 'unknown'

        # Prepare HBase columns
        columns = {}
        for key, value in record.items():
            if key != 'metric':
                columns[f'metrics:{key}'] = str(value)

        # Write to HBase
        table.put(row_key, columns)
        logger.info(f"Written to HBase table '{table_name}': {record}")
    except Exception as e:
        logger.error(f"Error writing to HBase table '{table_name}': {e}")
    finally:
        connection.close()

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("WriteEDAResultsToHBase") \
        .getOrCreate()

    logger.info("Spark session started.")

    kafka_bootstrap_servers = "kafka:9092"
    input_topic = "eda_results"

    # Define schema based on eda_results messages
    json_schema = StructType([
        StructField("metric", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand_name", StringType(), True),
        StructField("brand_count", IntegerType(), True),
        StructField("l1_category", StringType(), True),
        StructField("total_sales", DoubleType(), True),
        StructField("total_procured_quantity", IntegerType(), True),
        StructField("sales", DoubleType(), True),
        StructField("avg_unit_selling_price", DoubleType(), True),
        StructField("total_discount_amount", DoubleType(), True),
        StructField("total_orders", IntegerType(), True),
        StructField("total_quantity_sold", IntegerType(), True),
        StructField("average_order_value", DoubleType(), True),
        # Add other fields as necessary
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

    # Function to process each row and write to HBase
    def foreach_batch_function(df, epoch_id):
        if df.isEmpty():
            logger.info(f"No data received in epoch {epoch_id}.")
            return

        # Convert DataFrame to Pandas for iteration (not recommended for large data)
        # For better performance, consider using foreachPartition
        records = df.collect()

        for record in records:
            record_dict = record.asDict()
            metric = record_dict.get('metric')
            if metric:
                table_name = metric
                write_to_hbase(record_dict, table_name)
            else:
                logger.warning(f"Record without metric field: {record_dict}")

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
