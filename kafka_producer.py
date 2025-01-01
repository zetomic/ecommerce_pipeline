from kafka import KafkaProducer
import json
import csv
import time

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Absolute Path to your CSV file
csv_file_path = "D:/Projects/bdaproject/ecommerce.csv"

# Open the CSV file
with open(csv_file_path, mode='r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    
    # Send each row to the Kafka topic
    for row in csv_reader:
        try:
            # Send row as a JSON object
            producer.send("ecommerce", row)
            print(f"Sent: {row}")
            
            # Optional delay for throttling
            time.sleep(0.1)  # Adjust sleep time if needed
        except Exception as e:
            print(f"Error sending message: {e}")

# Ensure all messages are sent before exiting
producer.flush()
