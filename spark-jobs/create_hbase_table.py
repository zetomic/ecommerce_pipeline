import happybase
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_table():
    try:
        # Establish connection to HBase
        connection = happybase.Connection('hbase-master', port=9090)  # Replace with your HBase host and port
        connection.open()
        logger.info("Connected to HBase successfully.")

        table_name = 'ecommerce_data'
        column_families = {
            'product_data': {},    # For product-related fields
            'order_data': {},      # For order-related fields
            'customer_data': {},   # For customer-related fields
            'pricing': {},         # For pricing and discount fields
            'metadata': {}         # For metadata fields
        }

        # Check if table already exists
        if table_name.encode() not in connection.tables():
            connection.create_table(table_name, column_families)
            logger.info(f"Table '{table_name}' created successfully.")
        else:
            logger.info(f"Table '{table_name}' already exists.")

    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {e}")

    finally:
        connection.close()
        logger.info("HBase connection closed.")

if __name__ == "__main__":
    create_table()
